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
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
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
import java.util.Iterator;
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

   private static final Log log = LogFactory.getLog(BackupReceiverImpl.class);
   private static final boolean trace = log.isDebugEnabled();
   private final Cache<Object, Object> cache;

   private final BackupCacheUpdater siteUpdater;

   public BackupReceiverImpl(Cache<Object, Object> cache) {
      this.cache = cache;
      siteUpdater = new BackupCacheUpdater(cache);
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
      XSiteStateTransferControlCommand invokeCommand = command;
      if (!command.getCacheName().equals(cache.getName())) {
         //copy if the cache name is different
         invokeCommand = command.copyForCache(cache.getName());
      }
      invokeCommand.setSiteName(command.getOriginSite());
      invokeRemotelyInLocalSite(invokeCommand);
   }

   @Override
   public void handleStateTransferState(XSiteStatePushCommand cmd) throws Exception {
      //split the state and forward it to the primary owners...
      if (!cache.getStatus().allowInvocations()) {
         throw new CacheException("Cache is stopping or terminated: " + cache.getStatus());
      }
      final ClusteringDependentLogic clusteringDependentLogic = cache.getAdvancedCache().getComponentRegistry()
            .getComponent(ClusteringDependentLogic.class);
      final Map<Address, List<XSiteState>> primaryOwnersChunks = new HashMap<>();

      if (trace) {
         log.tracef("Received X-Site state transfer '%s'. Splitting by primary owner.", cmd);
      }

      for (XSiteState state : cmd.getChunk()) {
         final Address primaryOwner = clusteringDependentLogic.getPrimaryOwner(state.key());
         List<XSiteState> primaryOwnerList = primaryOwnersChunks.get(primaryOwner);
         if (primaryOwnerList == null) {
            primaryOwnerList = new LinkedList<>();
            primaryOwnersChunks.put(primaryOwner, primaryOwnerList);
         }
         primaryOwnerList.add(state);
      }

      final List<XSiteState> localChunks = primaryOwnersChunks.remove(clusteringDependentLogic.getAddress());
      final List<StatePushTask> tasks = new ArrayList<>(primaryOwnersChunks.size());

      for (Map.Entry<Address, List<XSiteState>> entry : primaryOwnersChunks.entrySet()) {
         if (entry.getValue() == null || entry.getValue().isEmpty()) {
            continue;
         }
         if (trace) {
            log.tracef("Node '%s' will apply %s", entry.getKey(), entry.getValue());
         }
         StatePushTask task = new StatePushTask(entry.getValue(), entry.getKey(), cache);
         tasks.add(task);
         task.executeRemote();
      }

      //help gc. this is safe because the chunks was already sent
      primaryOwnersChunks.clear();

      if (trace) {
         log.tracef("Local node '%s' will apply %s", cache.getAdvancedCache().getRpcManager().getAddress(),
                    localChunks);
      }

      if (localChunks != null) {
         LocalInvocation.newInstanceFromCache(cache, newStatePushCommand(cache, localChunks)).call();
         //help gc
         localChunks.clear();
      }

      if (trace) {
         log.tracef("Waiting for the remote tasks...");
      }

      while (!tasks.isEmpty()) {
         for (Iterator<StatePushTask> iterator = tasks.iterator(); iterator.hasNext(); ) {
            awaitRemoteTask(cache, iterator.next());
            iterator.remove();
         }
      }
      //the put operation can fail silently. check in the end and it is better to resend the chunk than to lose keys.
      if (!cache.getStatus().allowInvocations()) {
         throw new CacheException("Cache is stopping or terminated: " + cache.getStatus());
      }
   }

   private static void awaitRemoteTask(Cache<?, ?> cache, StatePushTask task) throws Exception {
      try {
         if (trace) {
            log.tracef("Waiting reply from %s", task.address);
         }
         Map<Address, Response> responseMap = task.awaitRemote();
         if (trace) {
            log.tracef("Response received is %s", responseMap);
         }
         if (responseMap.size() > 1 || !responseMap.containsKey(task.address)) {
            throw new IllegalStateException("Shouldn't happen. Map is " + responseMap);
         }
         Response response = responseMap.get(task.address);
         if (response == CacheNotFoundResponse.INSTANCE) {
            if (trace) {
               log.tracef("Cache not found in node '%s'. Retrying locally!", task.address);
            }
            if (!cache.getStatus().allowInvocations()) {
               throw new CacheException("Cache is stopping or terminated: " + cache.getStatus());
            }
            task.executeLocal();
         }
      } catch (Exception e) {
         if (!cache.getStatus().allowInvocations()) {
            throw new CacheException("Cache is stopping or terminated: " + cache.getStatus());
         }
         if (cache.getAdvancedCache().getRpcManager().getMembers().contains(task.address)) {
            if (trace) {
               log.tracef(e, "An exception was sent by %s. Retrying!", task.address);
            }
            task.executeRemote(); //retry!
         } else {
            if (trace) {
               log.tracef(e, "An exception was sent by %s. Retrying locally!", task.address);
            }
            //if the node left the cluster, we apply the missing state. This avoids the site provider to re-send the
            //full chunk.
            task.executeLocal();
         }
      }
   }

   private static XSiteStatePushCommand newStatePushCommand(Cache<?,?> cache, List<XSiteState> stateList) {
      CommandsFactory commandsFactory = cache.getAdvancedCache().getComponentRegistry().getCommandsFactory();
      return commandsFactory.buildXSiteStatePushCommand(stateList.toArray(new XSiteState[stateList.size()]));
   }

   private Map<Address, Response> invokeRemotelyInLocalSite(CacheRpcCommand command) throws Exception {
      final RpcManager rpcManager = cache.getAdvancedCache().getRpcManager();
      final NotifyingNotifiableFuture<Map<Address, Response>> remoteFuture = new NotifyingFutureImpl<>();
      final Map<Address, Response> responseMap = new HashMap<>();
      rpcManager.invokeRemotelyInFuture(remoteFuture, null, command, rpcManager.getDefaultRpcOptions(true, false));
      responseMap.put(rpcManager.getAddress(), LocalInvocation.newInstanceFromCache(cache, command).call());
      //noinspection unchecked
      responseMap.putAll(remoteFuture.get());
      return responseMap;
   }

   public static final class BackupCacheUpdater extends AbstractVisitor {

      private static Log log = LogFactory.getLog(BackupCacheUpdater.class);

      private final ConcurrentMap<GlobalTransaction, GlobalTransaction> remote2localTx;

      private final AdvancedCache<Object, Object> backupCache;

      BackupCacheUpdater(Cache<Object, Object> backup) {
         //ignore return values on the backup
         this.backupCache = backup.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES, Flag.SKIP_XSITE_BACKUP);
         this.remote2localTx = new ConcurrentHashMap<>();
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

   private static class StatePushTask {
      private final List<XSiteState> chunk;
      private final Address address;
      private final Cache<?,?> cache;
      private volatile Future<Map<Address, Response>> remoteFuture;


      private StatePushTask(List<XSiteState> chunk, Address address, Cache<?, ?> cache) {
         this.chunk = chunk;
         this.address = address;
         this.cache = cache;
      }

      public void executeRemote() {
         final RpcManager rpcManager = cache.getAdvancedCache().getRpcManager();
         NotifyingNotifiableFuture<Map<Address, Response>> future = new NotifyingFutureImpl<>();
         remoteFuture = future;
         rpcManager.invokeRemotelyInFuture(future, Collections.singletonList(address), newStatePushCommand(cache, chunk),
                                           rpcManager.getDefaultRpcOptions(true));
      }

      public Response executeLocal() throws Exception {
         return LocalInvocation.newInstanceFromCache(cache, newStatePushCommand(cache, chunk)).call();
      }

      public Map<Address, Response> awaitRemote() throws Exception {
         Future<Map<Address, Response>> future = remoteFuture;
         if (future == null) {
            throw new NullPointerException("Should not happen!");
         }
         return future.get();
      }

   }
}
