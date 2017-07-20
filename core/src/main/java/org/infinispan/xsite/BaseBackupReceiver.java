package org.infinispan.xsite;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.metadata.Metadata;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;

/**
 * Common implementation logic for {@link org.infinispan.xsite.BackupReceiver}
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 * @since 7.1
 */
public abstract class BaseBackupReceiver implements BackupReceiver {

   protected final AdvancedCache<Object, Object> cache;
   protected final ByteString cacheName;
   protected final TimeService timeService;
   private final BackupCacheUpdater siteUpdater;

   protected BaseBackupReceiver(Cache<Object, Object> cache) {
      this.cache = cache.getAdvancedCache();
      this.cacheName = ByteString.fromString(cache.getName());
      this.timeService = this.cache.getComponentRegistry().getTimeService();
      siteUpdater = new BackupCacheUpdater(cache);
   }

   protected static XSiteStatePushCommand newStatePushCommand(AdvancedCache<?, ?> cache, List<XSiteState> stateList) {
      CommandsFactory commandsFactory = cache.getComponentRegistry().getCommandsFactory();
      return commandsFactory.buildXSiteStatePushCommand(stateList.toArray(new XSiteState[stateList.size()]), 0);
   }

   @Override
   public final Cache getCache() {
      return cache;
   }

   @Override
   public final Object handleRemoteCommand(VisitableCommand command) throws Throwable {
      return command.acceptVisitor(null, siteUpdater);
   }

   protected final void assertAllowInvocation() {
      ComponentStatus status = cache.getStatus();
      if (!status.allowInvocations()) {
         throw new CacheException("Cache is stopping or terminated: " + status);
      }
   }

   private static final class BackupCacheUpdater extends AbstractVisitor {

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
            backupCache.putIfAbsent(command.getKey(), command.getValue(), command.getMetadata());
         } else {
            backupCache.put(command.getKey(), command.getValue(), command.getMetadata());
         }
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         if (command.isConditional()) {
            backupCache.remove(command.getKey(), command.getValue());
         } else {
            backupCache.remove(command.getKey());
         }
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         if (command.isConditional() && command.getOldValue() != null) {
            backupCache.replace(command.getKey(), command.getOldValue(), command.getNewValue(),
                                       command.getMetadata());
         } else {
            backupCache.replace(command.getKey(), command.getNewValue(),
                                command.getMetadata());
         }
         return null;
      }

      @Override
      public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
         if (command.isConditional()) {
            return backupCache.computeIfPresent(command.getKey(), command.getRemappingBiFunction(), command.getMetadata());
         }
         return backupCache.compute(command.getKey(), command.getRemappingBiFunction(), command.getMetadata());
      }

      @Override
      public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
         return backupCache.computeIfAbsent(command.getKey(), command.getMappingFunction(), command.getMetadata());
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
            if (!command.hasModifications()) {
               throw new IllegalStateException("TxInvocationContext has no modifications!");
            }

            replayModificationsInTransaction(command, command.isOnePhaseCommit());
         } else {
            replayModifications(command);
         }
         return null;
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
         } finally {
            LocalTransaction localTx = txTable().getLocalTransaction(tm.getTransaction());
            if (localTx != null) { //possible for the tx to be null if we got an exception during applying modifications
               localTx.setFromRemoteSite(true);

               if (onePhaseCommit) {
                  if (replaySuccessful) {
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
         return backupCache.getTransactionManager();
      }

      private void replayModifications(PrepareCommand command) throws Throwable {
         for (WriteCommand c : command.getModifications()) {
            c.acceptVisitor(null, this);
         }
      }
   }

}
