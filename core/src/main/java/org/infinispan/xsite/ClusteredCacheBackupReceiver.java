package org.infinispan.xsite;

import static org.infinispan.factories.KnownComponentNames.BLOCKING_EXECUTOR;
import static org.infinispan.remoting.transport.impl.MapResponseCollector.validOnly;
import static org.infinispan.util.concurrent.CompletableFutures.asCompletionException;
import static org.infinispan.util.concurrent.CompletableFutures.completedExceptionFuture;
import static org.infinispan.util.logging.Log.XSITE;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.core.MarshallableFunctions;
import org.infinispan.metadata.Metadata;
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
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.ActionSequencer;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.commands.XSiteStateTransferFinishReceiveCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartReceiveCommand;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;

/**
 * {@link org.infinispan.xsite.BackupReceiver} implementation for clustered caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class ClusteredCacheBackupReceiver implements BackupReceiver {

   private static final Log log = LogFactory.getLog(ClusteredCacheBackupReceiver.class);
   private static final boolean trace = log.isDebugEnabled();
   private final AdvancedCache<Object, Object> cache;
   private final ByteString cacheName;
   private final TimeService timeService;
   private final DefaultHandler defaultHandler;
   private final AsyncBackupHandler asyncBackupHandler;
   private final CommandsFactory commandsFactory;

   ClusteredCacheBackupReceiver(Cache<Object, Object> cache) {
      this.cache = cache.getAdvancedCache();
      this.cacheName = ByteString.fromString(cache.getName());
      ComponentRegistry registry = this.cache.getComponentRegistry();
      this.timeService = registry.getTimeService();
      this.commandsFactory = registry.getCommandsFactory();
      ExecutorService executor = registry.getComponent(ExecutorService.class, BLOCKING_EXECUTOR);
      TransactionHandler txHandler = new TransactionHandler(cache);
      boolean isVersionedTx = Configurations.isTxVersioned(cache.getCacheConfiguration());
      this.defaultHandler = new DefaultHandler(txHandler, executor, isVersionedTx);
      this.asyncBackupHandler = new AsyncBackupHandler(txHandler, executor, timeService, isVersionedTx);
   }

   @Override
   public final Cache getCache() {
      return cache;
   }

   @Override
   public CompletionStage<Void> handleStartReceivingStateTransfer(XSiteStateTransferStartReceiveCommand command) {
      return invokeRemotelyInLocalSite(XSiteStateTransferStartReceiveCommand.copyForCache(command, cacheName));
   }

   @Override
   public CompletionStage<Void> handleEndReceivingStateTransfer(XSiteStateTransferFinishReceiveCommand command) {
      return invokeRemotelyInLocalSite(XSiteStateTransferFinishReceiveCommand.copyForCache(command, cacheName));
   }

   @Override
   public CompletionStage<Void> handleStateTransferState(XSiteStatePushCommand cmd) {
      //split the state and forward it to the primary owners...
      CompletableFuture<Void> allowInvocation = checkInvocationAllowedFuture();
      if (allowInvocation != null) {
         return allowInvocation;
      }

      final long endTime = timeService.expectedEndTime(cmd.getTimeout(), TimeUnit.MILLISECONDS);
      final ClusteringDependentLogic clusteringDependentLogic = cache.getComponentRegistry()
            .getComponent(ClusteringDependentLogic.class);
      final Map<Address, List<XSiteState>> primaryOwnersChunks = new HashMap<>();
      final Address localAddress = clusteringDependentLogic.getAddress();

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
         StatePushTask task = new StatePushTask(entry.getValue(), entry.getKey(), cache, endTime);
         task.executeRemote();
         cf.dependsOn(task);
      }

      //help gc. this is safe because the chunks was already sent
      primaryOwnersChunks.clear();

      if (trace) {
         log.tracef("Local node '%s' will apply %s", localAddress, localChunks);
      }

      if (localChunks != null) {
         StatePushTask task = new StatePushTask(localChunks, localAddress, cache, endTime);
         task.executeLocal();
         cf.dependsOn(task);
      }

      return cf.freeze().thenApply(this::assertAllowInvocationFunction);
   }

   @Override
   public final CompletionStage<Void> handleRemoteCommand(VisitableCommand command, boolean preserveOrder) {
      try {
         DefaultHandler visitor = preserveOrder ? asyncBackupHandler : defaultHandler;
         //noinspection unchecked
         return (CompletableFuture<Void>) command.acceptVisitor(null, visitor);
      } catch (Throwable throwable) {
         return completedExceptionFuture(throwable);
      }
   }

   private <T> CompletableFuture<T> checkInvocationAllowedFuture() {
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

   private CompletionStage<Void> invokeRemotelyInLocalSite(CacheRpcCommand command) {
      final RpcManager rpcManager = cache.getRpcManager();
      CompletionStage<Map<Address, Response>> remote = rpcManager
            .invokeCommandOnAll(command, validOnly(), rpcManager.getSyncRpcOptions());
      CompletionStage<Response> local = LocalInvocation.newInstanceFromCache(cache, command).callAsync();
      return CompletableFuture.allOf(remote.toCompletableFuture(), local.toCompletableFuture());
   }

   private static class DefaultHandler extends AbstractVisitor {

      final TransactionHandler txHandler;
      final ExecutorService executor;
      final boolean dropVersion;

      private DefaultHandler(TransactionHandler txHandler, ExecutorService executor, boolean dropVersion) {
         this.txHandler = txHandler;
         this.executor = executor;
         this.dropVersion = dropVersion;
      }

      @Override
      public CompletionStage<Object> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
         Metadata metadata = dropVersionIfNeeded(command.getMetadata());
         return cache().putAsync(command.getKey(), command.getValue(), metadata);
      }

      @Override
      public CompletionStage<Object> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
         return cache().removeAsync(command.getKey());
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
         return CompletableFuture.runAsync(() -> txHandler.handlePrepareCommand(command), executor);
      }

      @Override
      public CompletionStage<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
         return CompletableFuture.runAsync(() -> txHandler.handleCommitCommand(command), executor);
      }

      @Override
      public CompletionStage<Void> visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) {
         return CompletableFuture.runAsync(() -> txHandler.handleRollbackCommand(command), executor);
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

      private Metadata dropVersionIfNeeded(Metadata metadata) {
         if (dropVersion && metadata != null) {
            return metadata.builder().version(null).build();
         }
         return metadata;
      }
   }

   private static final class AsyncBackupHandler extends DefaultHandler {

      private final ActionSequencer sequencer;

      private AsyncBackupHandler(TransactionHandler txHandler, ExecutorService executor, TimeService timeService,
            boolean dropVersion) {
         super(txHandler, executor, dropVersion);
         sequencer = new ActionSequencer(executor, false, timeService);
      }

      @Override
      public CompletionStage<Object> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
         assert !command.isConditional();
         Callable<CompletionStage<Object>> action = () -> super.visitPutKeyValueCommand(null, command);
         return sequencer.orderOnKey(command.getKey(), action);
      }

      @Override
      public CompletionStage<Object> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
         assert !command.isConditional();
         Callable<CompletionStage<Object>> action = () -> super.visitRemoveCommand(null, command);
         return sequencer.orderOnKey(command.getKey(), action);
      }

      @Override
      public CompletionStage<Void> visitWriteOnlyManyEntriesCommand(InvocationContext ctx,
            WriteOnlyManyEntriesCommand command) {
         Collection<?> keys = command.getAffectedKeys();
         Callable<CompletionStage<Void>> action = () -> super.visitWriteOnlyManyEntriesCommand(null, command);
         return sequencer.orderOnKeys(keys, action);
      }

      @Override
      public CompletionStage<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
         Collection<?> keys = command.getAffectedKeys();
         Callable<CompletionStage<Void>> action = () -> super.visitPrepareCommand(ctx, command);
         return sequencer.orderOnKeys(keys, action);
      }

      @Override
      public CompletionStage<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
         //we don't support async xsite with 2 phase commit
         throw new UnsupportedOperationException();
      }

      @Override
      public CompletionStage<Void> visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) {
         //we don't support async xsite with 2 phase commit
         throw new UnsupportedOperationException();
      }
   }

   // All conditional commands are unsupported
   private static final class TransactionHandler extends AbstractVisitor {

      private static final Log log = LogFactory.getLog(TransactionHandler.class);
      private static final boolean trace = log.isTraceEnabled();

      private final ConcurrentMap<GlobalTransaction, GlobalTransaction> remote2localTx;

      private final AdvancedCache<Object, Object> backupCache;
      private final FunctionalMap.WriteOnlyMap<Object, Object> writeOnlyMap;

      TransactionHandler(Cache<Object, Object> backup) {
         //ignore return values on the backup
         this.backupCache = backup.getAdvancedCache().withStorageMediaType().withFlags(Flag.IGNORE_RETURN_VALUES, Flag.SKIP_XSITE_BACKUP);
         this.writeOnlyMap = WriteOnlyMapImpl.create(FunctionalMapImpl.create(backupCache));
         this.remote2localTx = new ConcurrentHashMap<>();
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

      private TransactionTable txTable() {
         return backupCache.getComponentRegistry().getComponent(TransactionTable.class);
      }

      private boolean isTransactional() {
         return backupCache.getCacheConfiguration().transaction().transactionMode() == TransactionMode.TRANSACTIONAL;
      }

      private void completeTransaction(GlobalTransaction globalTransaction, boolean commit) throws Throwable {
         TransactionTable txTable = txTable();
         GlobalTransaction localTxId = remote2localTx.remove(globalTransaction);
         if (localTxId == null) {
            throw XSITE.unableToFindRemoteSiteTransaction(globalTransaction);
         }
         LocalTransaction localTx = txTable.getLocalTransaction(localTxId);
         if (localTx == null) {
            throw XSITE.unableToFindLocalTransactionFromRemoteSiteTransaction(globalTransaction);
         }
         TransactionManager txManager = txManager();
         txManager.resume(localTx.getTransaction());
         if (!localTx.isEnlisted()) {
            if (trace) {
               log.tracef("%s isn't enlisted! Removing it manually.", localTx);
            }
            txTable().removeLocalTransaction(localTx);
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
            LocalTransaction localTx = txTable().getLocalTransaction(tm.getTransaction());
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
      private final AdvancedCache<?, ?> cache;
      private final long endTime;


      private StatePushTask(List<XSiteState> chunk, Address address, AdvancedCache<?, ?> cache, long endTime) {
         this.chunk = chunk;
         this.address = address;
         this.cache = cache;
         this.endTime = endTime;
      }

      @Override
      public Void apply(Response response, Throwable throwable) {
         if (throwable != null) {
            if (isShouldGiveUp()) {
               return null;
            }

            RpcManager rpcManager = cache.getRpcManager();

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
         RpcManager rpcManager = cache.getRpcManager();
         RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
         rpcManager.invokeCommand(address, newStatePushCommand(chunk), this, rpcOptions)
               .handle(this);
      }

      private void executeLocal() {
         LocalInvocation.newInstanceFromCache(cache, newStatePushCommand(chunk)).callAsync()
               .handle(this);
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
