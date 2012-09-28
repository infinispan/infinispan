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

import org.infinispan.Cache;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.SitesConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class BackupSenderImpl implements BackupSender {

   private static Log log = LogFactory.getLog(BackupSenderImpl.class);

   private Cache cache;
   private Transport transport;
   private Configuration config;
   private TransactionTable txTable;
   private final Map<String, CustomFailurePolicy> siteFailurePolicy = new HashMap<String, CustomFailurePolicy>();


   private final String localSiteName;
   private String cacheName;
   private GlobalConfiguration globalConfig;

   public BackupSenderImpl(String localSiteName) {
      this.localSiteName = localSiteName;
   }

   @Inject
   public void init(Cache cache, Transport transport, TransactionTable txTable, GlobalConfiguration gc) {
      this.cache = cache;
      this.transport = transport;
      this.txTable = txTable;
      this.globalConfig = gc;
   }

   @Start
   public void start() {
      this.config = cache.getCacheConfiguration();
      this.cacheName = cache.getName();
      for (BackupConfiguration bc : config.sites().inUseBackups()) {
         if (bc.backupFailurePolicy() == BackupFailurePolicy.CUSTOM) {
            String backupPolicy = bc.failurePolicyClass();
            if (backupPolicy == null) {
               throw new IllegalStateException("Backup policy class missing for custom failure policy!");
            }
            CustomFailurePolicy instance = Util.getInstance(backupPolicy, globalConfig.classLoader());
            siteFailurePolicy.put(bc.site(), instance);
         }
      }
   }


   enum BackupFilter {KEEP_SYNC_ONLY, KEEP_ASYNC_ONLY, KEEP_ALL}

   @Override
   public BackupResponse backupPrepare(PrepareCommand command) throws Exception {
      //if we run a 2PC then filter out ASYNC prepare backup calls as they will happen during the local commit phase
      // as we know the tx is doomed to succeed.
      BackupFilter filter = !command.isOnePhaseCommit() ? BackupFilter.KEEP_SYNC_ONLY : BackupFilter.KEEP_ALL;
      List<XSiteBackup> backups = calculateBackupInfo(filter);
      return backupCommand(command, backups);

   }

   @Override
   public void processResponses(BackupResponse backupResponse, VisitableCommand command) throws Throwable {
      processResponses(backupResponse, command, null);
   }

   @Override
   public void processResponses(BackupResponse backupResponse, VisitableCommand command, Transaction transaction) throws Throwable {
      backupResponse.waitForBackupToFinish();
      SitesConfiguration sitesConfiguration = config.sites();
      Map<String, Exception> failures = backupResponse.getFailedBackups();
      for (Map.Entry<String, Exception> failure : failures.entrySet()) {
         BackupFailurePolicy policy = sitesConfiguration.getFailurePolicy(failure.getKey());
         if (policy == BackupFailurePolicy.CUSTOM) {
           CustomFailurePolicy customFailurePolicy = siteFailurePolicy.get(failure.getKey());
           command.acceptVisitor(null, new CustomBackupPolicyInvoker(failure.getKey(), customFailurePolicy, transaction));
         }
         if (policy == BackupFailurePolicy.WARN) {
            log.warnXsiteBackupFailed(cacheName, failure.getKey(), failure.getValue());
         } else if (policy == BackupFailurePolicy.FAIL) {
            throw new BackupFailureException(failure.getValue(),failure.getKey(), cacheName);
         }
      }
   }

   @Override
   public BackupResponse backupWrite(WriteCommand command) throws Exception {
      List<XSiteBackup> xSiteBackups = calculateBackupInfo(BackupFilter.KEEP_ALL);
      return backupCommand(command, xSiteBackups);
   }

   @Override
   public BackupResponse backupCommit(CommitCommand command) throws Exception {
      //we have a 2PC: we didn't backup the async stuff during prepare, we need to do it now.
      send1PcToAsyncBackups(command);
      List<XSiteBackup> xSiteBackups = calculateBackupInfo(BackupFilter.KEEP_SYNC_ONLY);
      return backupCommand(command, xSiteBackups);
   }

   @Override
   public BackupResponse backupRollback(RollbackCommand command) throws Exception {
      List<XSiteBackup> xSiteBackups = calculateBackupInfo(BackupFilter.KEEP_SYNC_ONLY);
      return backupCommand(command, xSiteBackups);
   }

   private BackupResponse backupCommand(ReplicableCommand command, List<XSiteBackup> xSiteBackups) throws Exception {
      return transport.backupRemotely(xSiteBackups, new SingleRpcCommand(cacheName, command));
   }

   private void send1PcToAsyncBackups(CommitCommand command) throws Exception {
      List<XSiteBackup> backups = calculateBackupInfo(BackupFilter.KEEP_ASYNC_ONLY);
      LocalTransaction localTx = txTable.getLocalTransaction(command.getGlobalTransaction());
      PrepareCommand prepare = new PrepareCommand(cacheName, localTx.getGlobalTransaction(),
                                                  localTx.getModifications(), true);
      backupCommand(prepare, backups);
   }

   private List<XSiteBackup> calculateBackupInfo(BackupFilter backupFilter) {
      List<XSiteBackup> backupInfo = new ArrayList<XSiteBackup>(2);
      SitesConfiguration sites = config.sites();
      for (BackupConfiguration bc : sites.inUseBackups()) {
         if (bc.site().equals(localSiteName)) {
            log.cacheBackupsDataToSameSite(localSiteName);
            continue;
         }
         boolean isSync = bc.strategy() == BackupConfiguration.BackupStrategy.SYNC;
         if (backupFilter == BackupFilter.KEEP_ASYNC_ONLY) {
            if (isSync) continue;
         }
         if (backupFilter == BackupFilter.KEEP_SYNC_ONLY) {
            if (!isSync)  continue;
         }
         XSiteBackup bi = new XSiteBackup(bc.site(), isSync, bc.replicationTimeout());
         backupInfo.add(bi);
      }
      return backupInfo;
   }

   public static final class CustomBackupPolicyInvoker extends AbstractVisitor {

      private final String site;
      private final CustomFailurePolicy failurePolicy;
      private final Transaction tx;

      public CustomBackupPolicyInvoker(String site, CustomFailurePolicy failurePolicy, Transaction tx) {
         this.site = site;
         this.failurePolicy = failurePolicy;
         this.tx = tx;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         failurePolicy.handlePutFailure(site, command.getKey(), command.getValue(), command.isPutIfAbsent());
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         failurePolicy.handleRemoveFailure(site, command.getKey(), command.getValue());
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         failurePolicy.handleReplaceFailure(site, command.getKey(), command.getOldValue(), command.getNewValue());
         return null;
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         failurePolicy.handleClearFailure(site);
         return null;
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         failurePolicy.handlePutAllFailure(site, command.getMap());
         return null;
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         failurePolicy.handlePrepareFailure(site, tx);
         return null;
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         failurePolicy.handleRollbackFailure(site, tx);
         return null;
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         failurePolicy.handleCommitFailure(site, tx);
         return null;
      }

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         super.handleDefault(ctx, command);
         throw new IllegalStateException("Unknown command: " + command);
      }
   }
}
