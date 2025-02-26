package org.infinispan.xsite;

import static org.infinispan.commons.util.concurrent.CompletableFutures.asCompletionException;
import static org.infinispan.context.Flag.IGNORE_RETURN_VALUES;
import static org.infinispan.context.Flag.SKIP_XSITE_BACKUP;
import static org.infinispan.remoting.transport.impl.MapResponseCollector.validOnly;
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
import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.concurrent.CompletableFutures;
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
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.irac.DiscardUpdateException;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.RollbackException;

/**
 * {@link org.infinispan.xsite.BackupReceiver} implementation for clustered caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
@Scope(Scopes.NAMED_CACHE)
public class ClusteredCacheBackupReceiver implements BackupReceiver {

   private static final Log log = LogFactory.getLog(ClusteredCacheBackupReceiver.class);
   private static final BiFunction<Object, Throwable, Void> CHECK_EXCEPTION = (o, throwable) -> {
      if (throwable == null || throwable instanceof DiscardUpdateException) {
         //for optimistic transaction, signals the update was discarded
         return null;
      }
      throw CompletableFutures.asCompletionException(throwable);
   };
   private static final long TRANSACTIONAL_FLAGS = EnumUtil.bitSetOf(IGNORE_RETURN_VALUES, SKIP_XSITE_BACKUP);

   @Inject Cache<Object, Object> cache;
   @Inject TimeService timeService;
   @Inject CommandsFactory commandsFactory;
   @Inject KeyPartitioner keyPartitioner;
   @Inject InvocationHelper invocationHelper;
   @Inject InvocationContextFactory invocationContextFactory;
   @Inject RpcManager rpcManager;
   @Inject ClusteringDependentLogic clusteringDependentLogic;

   private volatile DefaultHandler defaultHandler;

   public ClusteredCacheBackupReceiver() {
      //TODO #3 [ISPN-11824] split this class for pes/opt tx and non tx mode.
   }

   @Start
   public void start() {
      //it would be nice if we could inject bootstrap component
      //this feels kind hacky but saves 3 fields in this class
      ComponentRegistry cr =  ComponentRegistry.of(cache);
      TransactionHandler txHandler = new TransactionHandler(cache, cr.getTransactionTable(), invocationContextFactory, invocationHelper);
      defaultHandler = new DefaultHandler(txHandler, cr.getComponent(BlockingManager.class));
   }

   @Override
   public CompletionStage<Void> handleStateTransferControl(String originSite, boolean startReceiving) {
      CacheRpcCommand cmd = startReceiving ?
              commandsFactory.buildXSiteStateTransferStartReceiveCommand(originSite) :
              commandsFactory.buildXSiteStateTransferFinishReceiveCommand(originSite);
      return invokeRemotelyInLocalSite(cmd);
   }

   private static PrivateMetadata internalMetadata(IracMetadata metadata) {
      return new PrivateMetadata.Builder()
            .iracMetadata(metadata)
            .build();
   }

   @Override
   public CompletionStage<Void> handleStateTransferState(List<XSiteState> chunk, long timeoutMs) {
      //split the state and forward it to the primary owners...
      CompletableFuture<Void> allowInvocation = checkInvocationAllowedFuture();
      if (allowInvocation != null) {
         return allowInvocation;
      }

      long endTime = timeService.expectedEndTime(timeoutMs, TimeUnit.MILLISECONDS);
      Map<Address, List<XSiteState>> primaryOwnersChunks = new HashMap<>();
      Address localAddress = rpcManager.getAddress();

      if (log.isTraceEnabled()) {
         log.tracef("Received X-Site state transfer %s keys. Splitting by primary owner.", chunk.size());
      }

      for (XSiteState state : chunk) {
         Address primaryOwner = clusteringDependentLogic.getCacheTopology().getDistribution(state.key()).primary();
         List<XSiteState> primaryOwnerList = primaryOwnersChunks.computeIfAbsent(primaryOwner, k -> new LinkedList<>());
         primaryOwnerList.add(state);
      }

      List<XSiteState> localChunks = primaryOwnersChunks.remove(localAddress);
      AggregateCompletionStage<Void> cf = CompletionStages.aggregateCompletionStage();

      for (Map.Entry<Address, List<XSiteState>> entry : primaryOwnersChunks.entrySet()) {
         if (entry.getValue() == null || entry.getValue().isEmpty()) {
            continue;
         }
         if (log.isTraceEnabled()) {
            log.tracef("Node '%s' will apply %s", entry.getKey(), entry.getValue());
         }
         StatePushTask task = new StatePushTask(entry.getValue(), entry.getKey(), endTime);
         task.executeRemote();
         cf.dependsOn(task);
      }

      //help gc. this is safe because the chunks was already sent
      primaryOwnersChunks.clear();

      if (log.isTraceEnabled()) {
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
   public final <O> CompletionStage<O> handleRemoteCommand(VisitableCommand command) {
      try {
         //noinspection unchecked
         return (CompletionStage<O>) command.acceptVisitor(null, defaultHandler);
      } catch (Throwable throwable) {
         return CompletableFuture.failedFuture(throwable);
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
   public CompletionStage<Void> removeKey(Object key, IracMetadata iracMetadata, boolean expiration) {
      IracPutKeyValueCommand cmd = commandsFactory.buildIracPutKeyValueCommand(key, segment(key), null, null,
            internalMetadata(iracMetadata));
      cmd.setExpiration(expiration);
      InvocationContext ctx = invocationContextFactory.createSingleKeyNonTxInvocationContext();
      return invocationHelper.invokeAsync(ctx, cmd).handle(CHECK_EXCEPTION);
   }

   private <T> CompletableFuture<T> checkInvocationAllowedFuture() {
      //TODO #4 [ISPN-11824] no need to change the ComponentStatus. we have start/stop methods available now
      ComponentStatus status = cache.getStatus();
      if (!status.allowInvocations()) {
         return CompletableFuture.failedFuture(
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
      return commandsFactory.buildXSiteStatePushCommand(stateList);
   }

   @Override
   public CompletionStage<Void> clearKeys() {
      return defaultHandler.cache().clearAsync();
   }

   @Override
   public CompletionStage<Boolean> touchEntry(Object key) {
      return cache.getAdvancedCache().touch(key, false);
   }

   public boolean isTransactionTableEmpty() {
      return defaultHandler.txHandler.remote2localTx.isEmpty();
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

      private final ConcurrentMap<GlobalTransaction, GlobalTransaction> remote2localTx;

      private final AdvancedCache<Object, Object> backupCache;
      private final FunctionalMap.WriteOnlyMap<Object, Object> writeOnlyMap;
      private final TransactionTable transactionTable;
      private final InvocationContextFactory invocationContextFactory;
      private final InvocationHelper invocationHelper;

      TransactionHandler(Cache<Object, Object> backup, TransactionTable transactionTable, InvocationContextFactory invocationContextFactory, InvocationHelper invocationHelper) {
         //ignore return values on the backup
         backupCache = backup.getAdvancedCache().withStorageMediaType().withFlags(IGNORE_RETURN_VALUES, SKIP_XSITE_BACKUP);
         writeOnlyMap = WriteOnlyMapImpl.create(FunctionalMapImpl.create(backupCache));
         remote2localTx = new ConcurrentHashMap<>();
         this.transactionTable = transactionTable;
         this.invocationContextFactory = invocationContextFactory;
         this.invocationHelper = invocationHelper;
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
         // Sanity check -- if the remote tx doesn't have modifications, it never should have been propagated!
         if (!command.hasModifications()) {
            throw new IllegalStateException("TxInvocationContext has no modifications!");
         }
         if (isTransactional()) {
            try {
               replayModificationsInTransaction(command, command.isOnePhaseCommit());
            } catch (Throwable throwable) {
               // let's not sent JakartaEE/JavaEE transactions through the network
               throw CompletableFutures.asCompletionException(new CacheException(throwable.getMessage()));
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
            if (log.isTraceEnabled()) {
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
            if (log.isTraceEnabled()) {
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
         var globalTx = remote2localTx.remove(globalTransaction);
         if (globalTx == null) {
            if (commit) {
               throw XSITE.unableToFindRemoteSiteTransaction(globalTransaction);
            }
            return;
         }
         var localTx = transactionTable.getLocalTransaction(globalTx);
         if (localTx == null) {
            if (commit) {
               throw XSITE.unableToFindLocalTransactionFromRemoteSiteTransaction(globalTransaction);
            }
            return;
         }
         EmbeddedTransaction tx = (EmbeddedTransaction) localTx.getTransaction();
         if (!localTx.isEnlisted()) {
            if (log.isTraceEnabled()) {
               log.tracef("%s isn't enlisted! Removing it manually.", localTx);
            }
            transactionTable.removeLocalTransaction(localTx);
         }
         tx.runCommit(!commit);
      }

      private void replayModificationsInTransaction(PrepareCommand command, boolean onePhaseCommit) throws Exception {
         var tx = createTransaction();
         var localTx = transactionTable.getOrCreateLocalTransaction(tx, false);

         replayModificationsWithTransaction(tx, localTx, command);
         localTx.setFromRemoteSite(true);
         throwExceptionIfFailed(tx, localTx);

         if (onePhaseCommit) {
            runOnePhaseCommitAfterPrepare(tx, localTx);
            return;
         }

         // prepare only
         try {
            if (tx.runPrepare()) {
               // store tx for the commit/rollback
               remote2localTx.put(command.getGlobalTransaction(), localTx.getGlobalTransaction());
               return;
            }
         } catch (Throwable t) {
            tx.transactionFailed(t);
         }

         assert tx.getRollbackException() != null;
         throwExceptionIfFailed(tx, localTx);
      }

      private void replayModifications(PrepareCommand command) throws Throwable {
         for (WriteCommand c : command.getModifications()) {
            c.acceptVisitor(null, this);
         }
      }

      private void replayModificationsWithTransaction(EmbeddedTransaction tx, LocalTransaction localTx, PrepareCommand command) {
         var ctx = invocationContextFactory.createTxInvocationContext(localTx);
         var stage = CompletionStages.aggregateCompletionStage();
         try {
            for (WriteCommand c : command.getModifications()) {
               c.setFlagsBitSet(EnumUtil.mergeBitSets(TRANSACTIONAL_FLAGS, c.getFlagsBitSet()));
               stage.dependsOn(invocationHelper.invokeAsync(ctx, c).exceptionally(throwable -> {
                  tx.transactionFailed(throwable);
                  return null;
               }));
            }
            CompletionStages.await(stage.freeze());
         } catch (Throwable t) {
            tx.transactionFailed(t);
         }
      }

      private void throwExceptionIfFailed(EmbeddedTransaction tx, LocalTransaction localTx) throws RollbackException {
         if (tx.getRollbackException() != null) {
            try {
               tx.rollback();
            } catch (Throwable t) {
               //ignored, RollbackException will be thrown below
            }
            if (!localTx.isEnlisted()) {
               transactionTable.removeLocalTransaction(localTx);
            }
            assert !transactionTable.containsLocalTx(localTx.getGlobalTransaction());
            throw tx.getRollbackException();
         }
      }

      private void runOnePhaseCommitAfterPrepare(EmbeddedTransaction tx, LocalTransaction localTx) throws HeuristicRollbackException, HeuristicMixedException, RollbackException {
         try {
            tx.commit();
         } finally {
            if (!localTx.isEnlisted()) {
               transactionTable.removeLocalTransaction(localTx);
            }
            assert !transactionTable.containsLocalTx(localTx.getGlobalTransaction());
         }
      }

      private EmbeddedTransaction createTransaction() {
         return new EmbeddedTransaction(EmbeddedTransactionManager.getInstance());
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

            if (rpcManager.getMembers().contains(address) && !rpcManager.getAddress().equals(address)) {
               if (log.isTraceEnabled()) {
                  log.tracef(throwable, "An exception was sent by %s. Retrying!", address);
               }
               executeRemote(); //retry remote
            } else {
               if (log.isTraceEnabled()) {
                  log.tracef(throwable, "An exception was sent by %s. Retrying locally!", address);
               }
               //if the node left the cluster, we apply the missing state. This avoids the site provider to re-send the
               //full chunk.
               executeLocal(); //retry locally
            }
         } else if (response == CacheNotFoundResponse.INSTANCE) {
            if (log.isTraceEnabled()) {
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
