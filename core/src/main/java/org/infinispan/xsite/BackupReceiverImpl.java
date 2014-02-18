package org.infinispan.xsite;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.concurrent.NotifyingFutureImpl;
import org.infinispan.commons.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.Metadata;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.LocalInvocation;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;

import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class BackupReceiverImpl implements BackupReceiver {

   private final Cache<Object, Object> cache;

   //todo add some housekeeping logic for this, e.g. timeouts..
   private final ConcurrentMap<GlobalTransaction, GlobalTransaction> remote2localTx = new ConcurrentHashMap<GlobalTransaction, GlobalTransaction>();
   private final BackupCacheUpdater siteUpdater;

   @SuppressWarnings("unchecked")
   public BackupReceiverImpl(Cache cache) {
      this.cache = cache;
      siteUpdater = new BackupCacheUpdater(cache, remote2localTx);
   }

   @Override
   public Cache getCache() {
      return cache;
   }

   @Override
   public Object handleRemoteCommand(VisitableCommand command) throws Throwable {
      return command.acceptVisitor(null, siteUpdater);
   }

   @Override
   public void handleStateTransferControl(XSiteStateTransferControlCommand command) throws Exception {
      invokeRemotelyInLocalSite(command);
   }

   @Override
   public void handleStateTransferState(XSiteStatePushCommand cmd) throws Exception {
      //split the state and forward it to the primary owners...
      final ClusteringDependentLogic clusteringDependentLogic = cache.getAdvancedCache().getComponentRegistry()
            .getComponent(ClusteringDependentLogic.class);
      final Map<Address, List<XSiteState>> primaryOwnersChunks = new HashMap<Address, List<XSiteState>>();

      for (XSiteState state : cmd.getChunk()) {
         final Address primaryOwner = clusteringDependentLogic.getPrimaryOwner(state.key());
         List<XSiteState> primaryOwnerList = primaryOwnersChunks.get(primaryOwner);
         if (primaryOwnerList == null) {
            primaryOwnerList = new LinkedList<XSiteState>();
            primaryOwnersChunks.put(primaryOwner, primaryOwnerList);
         }
         primaryOwnerList.add(state);
      }

      final List<XSiteState> localChunks = primaryOwnersChunks.remove(clusteringDependentLogic.getAddress());
      final List<Future<?>> remoteFutures = new ArrayList<Future<?>>(primaryOwnersChunks.size());

      for (Map.Entry<Address, List<XSiteState>> entry : primaryOwnersChunks.entrySet()) {
         sendStateTo(entry.getKey(), entry.getValue(), remoteFutures);
      }

      if (localChunks != null) {
         LocalInvocation.newInstanceFromCache(cache, newStatePushCommand(localChunks)).call();
         //help gc
         localChunks.clear();
      }

      //help gc. this is safe because the chunks was already sent
      primaryOwnersChunks.clear();
      for (Future<?> future : remoteFutures) {
         future.get();
      }
   }

   private void sendStateTo(Address primaryOwner, List<XSiteState> stateList, List<Future<?>> remoteFutures) {
      if (stateList == null || stateList.isEmpty()) {
         return;
      }
      final RpcManager rpcManager = cache.getAdvancedCache().getRpcManager();
      final NotifyingNotifiableFuture<Object> future = new NotifyingFutureImpl<Object>();
      remoteFutures.add(future);
      rpcManager.invokeRemotelyInFuture(Collections.singletonList(primaryOwner), newStatePushCommand(stateList),                                        rpcManager.getDefaultRpcOptions(true), future);
   }

   private XSiteStatePushCommand newStatePushCommand(List<XSiteState> stateList) {
      CommandsFactory commandsFactory = cache.getAdvancedCache().getComponentRegistry().getCommandsFactory();
      return commandsFactory.buildXSiteStatePushCommand(stateList.toArray(new XSiteState[stateList.size()]));
   }

   private Map<Address, Response> invokeRemotelyInLocalSite(CacheRpcCommand command) throws Exception {
      final RpcManager rpcManager = cache.getAdvancedCache().getRpcManager();
      final NotifyingNotifiableFuture<Object> remoteFuture = new NotifyingFutureImpl<Object>();
      final Map<Address, Response> responseMap = new HashMap<Address, Response>();
      rpcManager.invokeRemotelyInFuture(null, command, rpcManager.getDefaultRpcOptions(true, false), remoteFuture);
      responseMap.put(rpcManager.getAddress(), LocalInvocation.newInstanceFromCache(cache, command).call());
      //noinspection unchecked
      responseMap.putAll((Map<? extends Address, ? extends Response>) remoteFuture.get());
      return responseMap;
   }

   public static final class BackupCacheUpdater extends AbstractVisitor {

      private static Log log = LogFactory.getLog(BackupCacheUpdater.class);

      private final ConcurrentMap<GlobalTransaction, GlobalTransaction> remote2localTx;

      private final AdvancedCache<Object, Object> backupCache;

      BackupCacheUpdater(Cache backup, ConcurrentMap<GlobalTransaction, GlobalTransaction> remote2localTx) {
         //ignore return values on the backup
         this.backupCache = backup.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES, Flag.SKIP_XSITE_BACKUP);
         this.remote2localTx = remote2localTx;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         log.tracef("Processing a remote put %s", command);
         if (command.isConditional()) {
            return backupCache.putIfAbsent(command.getKey(), command.getValue(), command.getMetadata());
         }
         return backupCache.put(command.getKey(), command.getValue(), command.getMetadata());
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         if (command.isConditional()) {
            return backupCache.remove(command.getKey(), command.getValue());
         }
         return backupCache.remove(command.getKey());
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         if (command.isConditional() && command.getOldValue() != null) {
            return backupCache.replace(command.getKey(), command.getOldValue(), command.getNewValue(),
                                       command.getMetadata());
         }
         return backupCache.replace(command.getKey(), command.getNewValue(),
                                    command.getMetadata());
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         Metadata metadata = command.getMetadata();
         backupCache.putAll(command.getMap(),
                            metadata.lifespan(), TimeUnit.MILLISECONDS,
                            metadata.maxIdle(), TimeUnit.MILLISECONDS);
         return null;
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         backupCache.clear();
         return null;
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         boolean isTransactional = isTransactional();
         if (isTransactional) {

            // Sanity check -- if the remote tx doesn't have modifications, it never should have been propagated!
            if( !command.hasModifications() ) {
               throw new IllegalStateException( "TxInvocationContext has no modifications!" );
            }

            replayModificationsInTransaction(command, command.isOnePhaseCommit());
         } else {
            replayModifications(command);
         }
         return null;
      }

      private boolean isTransactional() {
         return backupCache.getCacheConfiguration().transaction().transactionMode() == TransactionMode.TRANSACTIONAL;
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         if (!isTransactional()) {
            log.cannotRespondToCommit(command.getGlobalTransaction(), backupCache.getName());
         } else {
            log.tracef("Committing remote transaction %s", command.getGlobalTransaction());
            completeTransaction(command.getGlobalTransaction(), true);
         }
         return null;
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {

         if (!isTransactional()) {
            log.cannotRespondToRollback(command.getGlobalTransaction(), backupCache.getName());
         } else {
            log.tracef("Rolling back remote transaction %s", command.getGlobalTransaction());
            completeTransaction(command.getGlobalTransaction(), false);
         }
         return null;
      }

      private void completeTransaction(GlobalTransaction globalTransaction, boolean commit) throws Throwable {
         TransactionTable txTable = txTable();
         GlobalTransaction localTxId = remote2localTx.remove(globalTransaction);
         if (localTxId == null) {
            throw new CacheException("Couldn't find a local transaction corresponding to remote transaction " + globalTransaction);
         }
         LocalTransaction localTx = txTable.getLocalTransaction(localTxId);
         if (localTx == null) {
            throw new IllegalStateException("Local tx not found but present in the tx table!");
         }
         TransactionManager txManager = txManager();
         txManager.resume(localTx.getTransaction());
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
         }
         finally {
            LocalTransaction localTx = txTable().getLocalTransaction( tm.getTransaction() );
            if (localTx != null) { //possible for the tx to be null if we got an exception during applying modifications
               localTx.setFromRemoteSite(true);

               if (onePhaseCommit) {
                  if( replaySuccessful ) {
                     log.tracef("Committing remotely originated tx %s as it is 1PC", command.getGlobalTransaction());
                     tm.commit();
                  } else {
                     log.tracef("Rolling back remotely originated tx %s", command.getGlobalTransaction());
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
         return backupCache.getAdvancedCache().getTransactionManager();
      }

      public TransactionTable txTable() {
         return backupCache.getComponentRegistry().getComponent(TransactionTable.class);
      }

      private void replayModifications(PrepareCommand command) throws Throwable {
         for (WriteCommand c : command.getModifications()) {
            c.acceptVisitor(null, this);
         }
      }
   }
}
