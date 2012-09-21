/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.xsite;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheException;
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
import org.infinispan.configuration.cache.SitesConfiguration;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.TransactionManager;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Component present on a backup site that manages the backup information and logic.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class BackupReceiver {

   private final Cache cache;

   //todo add some housekeeping logic for this, e.g. timeouts..
   private final ConcurrentMap<GlobalTransaction, GlobalTransaction> remote2localTx = new ConcurrentHashMap<GlobalTransaction, GlobalTransaction>();
   private final BackupCacheUpdater siteUpdater;

   public BackupReceiver(Cache cache) {
      this.cache = cache;
      siteUpdater = new BackupCacheUpdater(cache, remote2localTx);
   }

   public Cache getCache() {
      return cache;
   }

   public Object handleRemoteCommand(VisitableCommand command) throws Throwable {
      return command.acceptVisitor(null, siteUpdater);
   }

   public static final class BackupCacheUpdater extends AbstractVisitor {

      private static Log log = LogFactory.getLog(BackupCacheUpdater.class);

      private final ConcurrentMap<GlobalTransaction, GlobalTransaction> remote2localTx;

      private final AdvancedCache backupCache;

      BackupCacheUpdater(Cache backup, ConcurrentMap<GlobalTransaction, GlobalTransaction> remote2localTx) {
         //ignore return values on the backup
         this.backupCache = backup.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES, Flag.SKIP_XSITE_BACKUP);
         this.remote2localTx = remote2localTx;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         log.tracef("Processing a remote put %s", command);
         if (command.isConditional()) {
            return backupCache.putIfAbsent(command.getKey(), command.getValue(),
                                           command.getLifespanMillis(), TimeUnit.MILLISECONDS,
                                           command.getMaxIdleTimeMillis(), TimeUnit.MILLISECONDS);
         }
         return backupCache.put(command.getKey(), command.getValue(),
                                                                     command.getLifespanMillis(), TimeUnit.MILLISECONDS,
                                                                     command.getMaxIdleTimeMillis(), TimeUnit.MILLISECONDS);
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
                                       command.getLifespanMillis(), TimeUnit.MILLISECONDS,
                                       command.getMaxIdleTimeMillis(), TimeUnit.MILLISECONDS);
         }
         return backupCache.replace(command.getKey(), command.getNewValue(),
                                    command.getLifespanMillis(), TimeUnit.MILLISECONDS,
                                    command.getMaxIdleTimeMillis(), TimeUnit.MILLISECONDS);
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         backupCache.putAll(command.getMap(), command.getLifespanMillis(), TimeUnit.MILLISECONDS,
                                   command.getMaxIdleTimeMillis(), TimeUnit.MILLISECONDS );
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
            log.cannotRespondToRollback(command.getGlobalTransaction(), backupCache.getName());
         } else {
            log.tracef("Rolling back remote transaction %s", command.getGlobalTransaction());
            completeTransaction(command.getGlobalTransaction(), true);
         }
         return null;
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         completeTransaction(command.getGlobalTransaction(), false);
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
         tm.begin();
         replayModifications(command);
         LocalTransaction localTx = txTable().getLocalTransaction(tm.getTransaction());
         localTx.setFromRemoteSite(true);
         if (onePhaseCommit) {
            log.tracef("Committing remotely originated tx %s as it is 1PC", command.getGlobalTransaction());
            tm.commit();
         } else {
            remote2localTx.put(command.getGlobalTransaction(), localTx.getGlobalTransaction());
         }
         tm.suspend();
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
