package org.infinispan.xsite;

import static org.infinispan.context.Flag.IGNORE_RETURN_VALUES;
import static org.infinispan.context.Flag.SKIP_XSITE_BACKUP;
import static org.infinispan.remoting.transport.impl.MapResponseCollector.validOnly;
import static org.infinispan.util.concurrent.CompletableFutures.asCompletionException;
import static org.infinispan.util.concurrent.CompletableFutures.completedExceptionFuture;
import static org.infinispan.util.logging.Log.XSITE;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.InvocationHelper;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.core.MarshallableFunctions;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.remoting.LocalInvocation;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.commands.XSiteStateTransferFinishReceiveCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartReceiveCommand;
import org.infinispan.xsite.irac.DiscardUpdateException;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;

/**
 * {@link org.infinispan.xsite.BackupReceiver} implementation for clustered caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
@Scope(Scopes.NAMED_CACHE)
public class ClusteredCacheBackupReceiver implements BackupReceiver {

   private static final Log log = LogFactory.getLog(ClusteredCacheBackupReceiver.class);
   private static final boolean trace = log.isDebugEnabled();
   private static final BiFunction<Object, Throwable, Void> CHECK_EXCEPTION = (o, throwable) -> {
      if (throwable == null || throwable instanceof DiscardUpdateException) {
         //for optimistic transaction, signals the update was discarded
         return null;
      }
      throw CompletableFutures.asCompletionException(throwable);
   };

   @Inject Cache<Object, Object> cache;
   @Inject TimeService timeService;
   @Inject CommandsFactory commandsFactory;
   @Inject KeyPartitioner keyPartitioner;
   @Inject InvocationHelper invocationHelper;
   @Inject InvocationContextFactory invocationContextFactory;
   @Inject RpcManager rpcManager;
   @Inject ClusteringDependentLogic clusteringDependentLogic;
   @Inject ComponentRegistry componentRegistry;
   @Inject InternalDataContainer<Object, Object> dataContainer;

   private final ByteString cacheName;

   private volatile DefaultHandler defaultHandler;

   public ClusteredCacheBackupReceiver(String cacheName) {
      //TODO #3 [ISPN-11824] split this class for pes/opt tx and non tx mode.
      this.cacheName = ByteString.fromString(cacheName);
   }

   @Start
   public void start() {
      //it would be nice if we could inject bootstrap component
      //this feels kind hacky but saves 3 fields in this class
      TransactionHandler txHandler = new TransactionHandler(cache, componentRegistry.getTransactionTable());
      defaultHandler = new DefaultHandler(txHandler, componentRegistry.getComponent(BlockingManager.class));
   }

   @Override
   public CompletionStage<Void> handleStartReceivingStateTransfer(XSiteStateTransferStartReceiveCommand command) {
      return invokeRemotelyInLocalSite(XSiteStateTransferStartReceiveCommand.copyForCache(command, cacheName));
   }

   @Override
   public CompletionStage<Void> handleEndReceivingStateTransfer(XSiteStateTransferFinishReceiveCommand command) {
      return invokeRemotelyInLocalSite(XSiteStateTransferFinishReceiveCommand.copyForCache(command, cacheName));
   }

   private static PrivateMetadata internalMetadata(IracMetadata metadata) {
      return new PrivateMetadata.Builder()
            .iracMetadata(metadata)
            .build();
   }

   @Override
   public CompletionStage<Void> handleStateTransferState(XSiteStatePushCommand cmd) {
      //split the state and forward it to the primary owners...
      CompletableFuture<Void> allowInvocation = checkInvocationAllowedFuture();
      if (allowInvocation != null) {
         return allowInvocation;
      }

      final long endTime = timeService.expectedEndTime(cmd.getTimeout(), TimeUnit.MILLISECONDS);
      final Map<Address, List<XSiteState>> primaryOwnersChunks = new HashMap<>();
      final Address localAddress = rpcManager.getAddress();

      if (trace) {
         log.tracef("Received X-Site state transfer '%s'. Splitting by primary owner.", cmd);
      }

      for (XSiteState state : cmd.getChunk()) {
         Address primaryOwner = clusteringDependentLogic.getCacheTopology().getDistribution(state.key()).primary();
         List<XSiteState> primaryOwnerList = primaryOwnersChunks.computeIfAbsent(primaryOwner, k -> new LinkedList<>());
         primaryOwnerList.add(state);
      }

      final List<XSiteState> localChunks = primaryOwnersChunks.remove(localAddress);
      AggregateCompletionStage<Void> cf = CompletionStages.aggregateCompletionStage();

      for (Map.Entry<Address, List<XSiteState>> entry : primaryOwnersChunks.entrySet()) {
         if (entry.getValue() == null || entry.getValue().isEmpty()) {
            continue;
         }
         if (trace) {
            log.tracef("Node '%s' will apply %s", entry.getKey(), entry.getValue());
         }
         StatePushTask task = new StatePushTask(entry.getValue(), entry.getKey(), endTime);
         task.executeRemote();
         cf.dependsOn(task);
      }

      //help gc. this is safe because the chunks was already sent
      primaryOwnersChunks.clear();

      if (trace) {
         log.tracef("Local node '%s' will apply %s", localAddress, localChunks);
      }

      if (localChunks != null) {
         StatePushTask task = new StatePushTask(localChunks, localAddress, endTime);
         task.executeLocal();
         cf.dependsOn(task);
      }

      return cf.freeze().thenApply(this::assertAllowInvocationFunction);
   }

   @Override
   public final <O> CompletionStage<O> handleRemoteCommand(VisitableCommand command, boolean preserveOrder) {
      try {
         //currently, it only handles sync xsite requests.
         //async xsite requests are handle by the other methods.
         assert !preserveOrder;
         //noinspection unchecked
         return (CompletionStage<O>) command.acceptVisitor(null, defaultHandler);
      } catch (Throwable throwable) {
         return completedExceptionFuture(throwable);
      }
   }

   @Override
   public CompletionStage<Void> putKeyValue(Object key, Object value, Metadata metadata, IracMetadata iracMetadata) {
      IracPutKeyValueCommand cmd = commandsFactory.buildIracPutKeyValueCommand(key, segment(key), value, metadata,
            internalMetadata(iracMetadata));
      InvocationContext ctx = invocationContextFactory.createSingleKeyNonTxInvocationContext();
      return invocationHelper.invokeAsync(ctx, cmd).handle(CHECK_EXCEPTION);
   }

   @Override
   public CompletionStage<Void> removeKey(Object key, IracMetadata iracMetadata) {
      IracPutKeyValueCommand cmd = commandsFactory.buildIracPutKeyValueCommand(key, segment(key), null, null,
            internalMetadata(iracMetadata));
      InvocationContext ctx = invocationContextFactory.createSingleKeyNonTxInvocationContext();
      return invocationHelper.invokeAsync(ctx, cmd).handle(CHECK_EXCEPTION);
   }

   private <T> CompletableFuture<T> checkInvocationAllowedFuture() {
      //TODO #4 [ISPN-11824] no need to change the ComponentStatus. we have start/stop methods available now
      ComponentStatus status = cache.getStatus();
      if (!status.allowInvocations()) {
         return completedExceptionFuture(
               new IllegalLifecycleStateException("Cache is stopping or terminated: " + status));
      }
      return null;
   }

   private Void assertAllowInvocationFunction(Object ignoredRetVal) {
      //the put operation can fail silently. check in the end and it is better to resend the chunk than to lose keys.
      ComponentStatus status = cache.getStatus();
      if (!status.allowInvocations()) {
         throw asCompletionException(new IllegalLifecycleStateException("Cache is stopping or terminated: " + status));
      }
      return null;
   }

   private XSiteStatePushCommand newStatePushCommand(List<XSiteState> stateList) {
      return commandsFactory.buildXSiteStatePushCommand(stateList.toArray(new XSiteState[0]), 0);
   }

   @Override
   public CompletionStage<Void> clearKeys() {
      return defaultHandler.cache().clearAsync();
   }

   @Override
   public CompletionStage<Boolean> touchEntry(Object key) {
      int segment = clusteringDependentLogic.getCacheTopology().getSegment(key);
      InternalCacheEntry<Object, Object> entry = dataContainer.peek(segment, key);
      long currentTime = timeService.wallClockTime();
      if (entry == null || entry.isExpired(currentTime)) {
         if (trace) {
            log.tracef("Entry was not found or is expired for key %s", key);
         }
         return CompletableFutures.completedFalse();
      }
      // TODO: do we touch other nodes in this cluster?
      if (trace) {
         log.tracef("Entry was found and wasn't expired for key %s", key);
      }
      dataContainer.touch(segment, key, currentTime);
      return CompletableFutures.completedTrue();
   }

   private CompletionStage<Void> invokeRemotelyInLocalSite(CacheRpcCommand command) {
      CompletionStage<Map<Address, Response>> remote = rpcManager
            .invokeCommandOnAll(command, validOnly(), rpcManager.getSyncRpcOptions());
      //TODO #5 [ISPN-11824] this allocations can be removed and invoke XSiteStateConsumer
      //handleStartReceivingStateTransfer and handleEndReceivingStateTransfer can be merged. both interact with XSiteStateConsumer.
      CompletionStage<Response> local = LocalInvocation.newInstanceFromCache(cache, command).callAsync();
      return CompletableFuture.allOf(remote.toCompletableFuture(), local.toCompletableFuture());
   }

   private int segment(Object key) {
      return keyPartitioner.getSegment(key);
   }

   private static class DefaultHandler extends AbstractVisitor {

      final TransactionHandler txHandler;
      final BlockingManager blockingManager;

      private DefaultHandler(TransactionHandler txHandler, BlockingManager blockingManager) {
         this.txHandler = txHandler;
         this.blockingManager = blockingManager;
      }

      @Override
      public CompletionStage<Object> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
         return cache().putAsync(command.getKey(), command.getValue(), command.getMetadata());
      }

      @Override
      public CompletionStage<Object> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
         return cache().removeAsync(command.getKey());
      }

      @Override
      public Object visitRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand command) {
         if (!command.isMaxIdle()) {
            throw new UnsupportedOperationException("Lifespan based expiration is not supported for xsite");
         }
         return cache().removeMaxIdleExpired(command.getKey(), command.getValue());
      }

      @Override
      public CompletionStage<Void> visitWriteOnlyManyEntriesCommand(InvocationContext ctx,
            WriteOnlyManyEntriesCommand command) {
         //noinspection unchecked
         return fMap().evalMany(command.getArguments(), MarshallableFunctions.setInternalCacheValueConsumer());
      }

      @Override
      public final CompletionStage<Void> visitClearCommand(InvocationContext ctx, ClearCommand command) {
         return cache().clearAsync();
      }

      @Override
      public CompletionStage<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
         return blockingManager.runBlocking(() -> txHandler.handlePrepareCommand(command), command.getCommandId());
      }

      @Override
      public CompletionStage<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
         return blockingManager.runBlocking(() -> txHandler.handleCommitCommand(command), command.getCommandId());
      }

      @Override
      public CompletionStage<Void> visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) {
         return blockingManager.runBlocking(() -> txHandler.handleRollbackCommand(command), command.getCommandId());
      }

      @Override
      protected final Object handleDefault(InvocationContext ctx, VisitableCommand command) {
         throw new UnsupportedOperationException();
      }

      private AdvancedCache<Object, Object> cache() {
         return txHandler.backupCache;
      }

      private FunctionalMap.WriteOnlyMap<Object, Object> fMap() {
         return txHandler.writeOnlyMap;
      }
   }

   // All conditional commands are unsupported
   private static final class TransactionHandler extends AbstractVisitor {

      private static final Log log = LogFactory.getLog(TransactionHandler.class);
      private static final boolean trace = log.isTraceEnabled();

      private final ConcurrentMap<GlobalTransaction, GlobalTransaction> remote2localTx;

      private final AdvancedCache<Object, Object> backupCache;
      private final FunctionalMap.WriteOnlyMap<Object, Object> writeOnlyMap;
      private final TransactionTable transactionTable;

      TransactionHandler(Cache<Object, Object> backup, TransactionTable transactionTable) {
         //ignore return values on the backup
         this.backupCache = backup.getAdvancedCache().withStorageMediaType().withFlags(IGNORE_RETURN_VALUES, SKIP_XSITE_BACKUP);
         this.writeOnlyMap = WriteOnlyMapImpl.create(FunctionalMapImpl.create(backupCache));
         this.remote2localTx = new ConcurrentHashMap<>();
         this.transactionTable = transactionTable;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
         if (command.isConditional()) {
            throw new UnsupportedOperationException();
         }
         backupCache.put(command.getKey(), command.getValue(), command.getMetadata());
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
         if (command.isConditional()) {
            throw new UnsupportedOperationException();
         }
         backupCache.remove(command.getKey());
         return null;
      }

      @Override
      public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) {
         CompletableFuture<?> future = writeOnlyMap
               .evalMany(command.getArguments(), MarshallableFunctions.setInternalCacheValueConsumer());
         return future.join();
      }

      void handlePrepareCommand(PrepareCommand command) {
         if (isTransactional()) {
            // Sanity check -- if the remote tx doesn't have modifications, it never should have been propagated!
            if (!command.hasModifications()) {
               throw new IllegalStateException("TxInvocationContext has no modifications!");
            }

            try {
               replayModificationsInTransaction(command, command.isOnePhaseCommit());
            } catch (Throwable throwable) {
               throw CompletableFutures.asCompletionException(throwable);
            }
         } else {
            try {
               replayModifications(command);
            } catch (Throwable throwable) {
               throw CompletableFutures.asCompletionException(throwable);
            }
         }
      }

      void handleCommitCommand(CommitCommand command) {
         if (!isTransactional()) {
            log.cannotRespondToCommit(command.getGlobalTransaction(), backupCache.getName());
         } else {
            if (trace) {
               log.tracef("Committing remote transaction %s", command.getGlobalTransaction());
            }
            try {
               completeTransaction(command.getGlobalTransaction(), true);
            } catch (Throwable throwable) {
               throw CompletableFutures.asCompletionException(throwable);
            }
         }
      }

      void handleRollbackCommand(RollbackCommand command) {
         if (!isTransactional()) {
            log.cannotRespondToRollback(command.getGlobalTransaction(), backupCache.getName());
         } else {
            if (trace) {
               log.tracef("Rolling back remote transaction %s", command.getGlobalTransaction());
            }
            try {
               completeTransaction(command.getGlobalTransaction(), false);
            } catch (Throwable throwable) {
               throw CompletableFutures.asCompletionException(throwable);
            }
         }
      }

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) {
         throw new UnsupportedOperationException();
      }

      private boolean isTransactional() {
         return transactionTable != null;
      }

      private void completeTransaction(GlobalTransaction globalTransaction, boolean commit) throws Throwable {
         GlobalTransaction localTxId = remote2localTx.remove(globalTransaction);
         if (localTxId == null) {
            throw XSITE.unableToFindRemoteSiteTransaction(globalTransaction);
         }
         LocalTransaction localTx = transactionTable.getLocalTransaction(localTxId);
         if (localTx == null) {
            throw XSITE.unableToFindLocalTransactionFromRemoteSiteTransaction(globalTransaction);
         }
         TransactionManager txManager = txManager();
         txManager.resume(localTx.getTransaction());
         if (!localTx.isEnlisted()) {
            if (trace) {
               log.tracef("%s isn't enlisted! Removing it manually.", localTx);
            }
            transactionTable.removeLocalTransaction(localTx);
         }
         if (commit) {
            txManager.commit();
         } else {
            txManager.rollback();
         }
      }

      private void replayModificationsInTransaction(PrepareCommand command, boolean onePhaseCommit) throws Throwable {
         TransactionManager tm = txManager();
         boolean replaySuccessful = false;
         try {

            tm.begin();
            replayModifications(command);
            replaySuccessful = true;
         } finally {
            LocalTransaction localTx = transactionTable.getLocalTransaction(tm.getTransaction());
            if (localTx != null) { //possible for the tx to be null if we got an exception during applying modifications
               localTx.setFromRemoteSite(true);

               if (onePhaseCommit) {
                  if (replaySuccessful) {
                     if (trace) {
                        log.tracef("Committing remotely originated tx %s as it is 1PC", command.getGlobalTransaction());
                     }
                     tm.commit();
                  } else {
                     if (trace) {
                        log.tracef("Rolling back remotely originated tx %s", command.getGlobalTransaction());
                     }
                     tm.rollback();
                  }
               } else { // Wait for a remote commit/rollback.
                  remote2localTx.put(command.getGlobalTransaction(), localTx.getGlobalTransaction());
                  tm.suspend();
               }
            }
         }
      }

      private TransactionManager txManager() {
         return backupCache.getTransactionManager();
      }

      private void replayModifications(PrepareCommand command) throws Throwable {
         for (WriteCommand c : command.getModifications()) {
            c.acceptVisitor(null, this);
         }
      }
   }

   private class StatePushTask extends CompletableFuture<Void>
         implements ResponseCollector<Response>, BiFunction<Response, Throwable, Void> {
      private final List<XSiteState> chunk;
      private final Address address;
      private final long endTime;


      private StatePushTask(List<XSiteState> chunk, Address address, long endTime) {
         this.chunk = chunk;
         this.address = address;
         this.endTime = endTime;
      }

      @Override
      public Void apply(Response response, Throwable throwable) {
         if (throwable != null) {
            if (isShouldGiveUp()) {
               return null;
            }

            if (rpcManager.getMembers().contains(this.address) && !rpcManager.getAddress().equals(this.address)) {
               if (trace) {
                  log.tracef(throwable, "An exception was sent by %s. Retrying!", this.address);
               }
               executeRemote(); //retry remote
            } else {
               if (trace) {
                  log.tracef(throwable, "An exception was sent by %s. Retrying locally!", this.address);
               }
               //if the node left the cluster, we apply the missing state. This avoids the site provider to re-send the
               //full chunk.
               executeLocal(); //retry locally
            }
         } else if (response == CacheNotFoundResponse.INSTANCE) {
            if (trace) {
               log.tracef("Cache not found in node '%s'. Retrying locally!", address);
            }
            if (isShouldGiveUp()) {
               return null;
            }
            executeLocal(); //retry locally
         } else {
            complete(null);
         }
         return null;
      }

      @Override
      public Response addResponse(Address sender, Response response) {
         if (response instanceof ValidResponse || response instanceof CacheNotFoundResponse) {
            return response;
         } else if (response instanceof ExceptionResponse) {
            throw ResponseCollectors.wrapRemoteException(sender, ((ExceptionResponse) response).getException());
         } else {
            throw ResponseCollectors
                  .wrapRemoteException(sender, new RpcException("Unknown response type: " + response));
         }
      }

      @Override
      public Response finish() {
         return null;
      }

      private void executeRemote() {
         RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
         rpcManager.invokeCommand(address, newStatePushCommand(chunk), this, rpcOptions).handle(this);
      }

      private void executeLocal() {
         //TODO #1 [ISPN-11824] make state transfer non blocking
         //TODO #2 [ISPN-11824] avoid all this allocations by invoking XSiteStateConsumer.apply() directly
         LocalInvocation.newInstanceFromCache(cache, newStatePushCommand(chunk)).callAsync().handle(this);
      }

      /**
       * @return {@code null} if it can retry
       */
      private boolean isShouldGiveUp() {
         ComponentStatus status = cache.getStatus();
         if (!status.allowInvocations()) {
            completeExceptionally(new IllegalLifecycleStateException("Cache is stopping or terminated: " + status));
            return true;
         }
         if (timeService.isTimeExpired(endTime)) {
            completeExceptionally(new TimeoutException("Unable to apply state in the time limit."));
            return true;
         }
         return false;
      }
   }
}
