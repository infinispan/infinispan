package org.infinispan.xsite;

import org.infinispan.Cache;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
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
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.SitesConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.transport.AggregateBackupResponse;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class BackupSenderImpl implements BackupSender {

   private static Log log = LogFactory.getLog(BackupSenderImpl.class);
   private static final BackupResponse EMPTY_RESPONSE = new EmptyBackupResponse();

   private Cache cache;
   private Transport transport;
   private Configuration config;
   private TransactionTable txTable;
   private TimeService timeService;
   private CommandsFactory commandsFactory;
   private final Map<String, CustomFailurePolicy> siteFailurePolicy = new HashMap<String, CustomFailurePolicy>();
   private final ConcurrentMap<String, OfflineStatus> offlineStatus = CollectionFactory.makeConcurrentMap();


   private final String localSiteName;
   private String cacheName;
   private GlobalConfiguration globalConfig;

   private enum BackupFilter {KEEP_1PC_ONLY, KEEP_2PC_ONLY, KEEP_ALL}

   public BackupSenderImpl(String localSiteName) {
      this.localSiteName = localSiteName;
   }

   @Inject
   public void init(Cache cache, Transport transport, TransactionTable txTable, GlobalConfiguration gc,
                    TimeService timeService, CommandsFactory commandsFactory) {
      this.cache = cache;
      this.transport = transport;
      this.txTable = txTable;
      this.globalConfig = gc;
      this.timeService = timeService;
      this.commandsFactory = commandsFactory;
   }

   @Start
   public void start() {
      this.config = cache.getCacheConfiguration();
      this.cacheName = cache.getName();
      for (BackupConfiguration bc : config.sites().enabledBackups()) {
         if (bc.backupFailurePolicy() == BackupFailurePolicy.CUSTOM) {
            String backupPolicy = bc.failurePolicyClass();
            if (backupPolicy == null) {
               throw new IllegalStateException("Backup policy class missing for custom failure policy!");
            }
            CustomFailurePolicy instance = Util.getInstance(backupPolicy, globalConfig.classLoader());
            instance.init(cache);
            siteFailurePolicy.put(bc.site(), instance);
         }
         OfflineStatus offline = new OfflineStatus(bc.takeOffline(), timeService);
         offlineStatus.put(bc.site(), offline);
      }
   }

   @Override
   public BackupResponse backupPrepare(PrepareCommand command) throws Exception {
      List<WriteCommand> modifications = filterModifications(command.getModifications());
      if (modifications.isEmpty()) {
         return EMPTY_RESPONSE;
      }
      PrepareCommand prepare = commandsFactory.buildPrepareCommand(command.getGlobalTransaction(), modifications,
                                                                   command.isOnePhaseCommit());
      //if we run a 2PC then filter out 1PC prepare backup calls as they will happen during the local commit phase.
      BackupFilter filter = !prepare.isOnePhaseCommit() ? BackupFilter.KEEP_2PC_ONLY : BackupFilter.KEEP_ALL;
      List<XSiteBackup> backups = calculateBackupInfo(filter);
      return backupCommand(prepare, backups);
   }

   @Override
   public void processResponses(BackupResponse backupResponse, VisitableCommand command) throws Throwable {
      processResponses(backupResponse, command, null);
   }

   @Override
   public void processResponses(BackupResponse backupResponse, VisitableCommand command, Transaction transaction) throws Throwable {
      log.tracef("Processing backup response %s for command %s", backupResponse, command);
      backupResponse.waitForBackupToFinish();
      updateOfflineSites(backupResponse);
      processFailedResponses(backupResponse, command, transaction);
   }

   private void updateOfflineSites(BackupResponse backupResponse) {
      if (offlineStatus.isEmpty() || backupResponse.isEmpty()) return;
      Set<String> communicationErrors = backupResponse.getCommunicationErrors();
      for (Map.Entry<String, OfflineStatus> statusEntry : offlineStatus.entrySet()) {
         OfflineStatus status = statusEntry.getValue();
         if (!status.isEnabled()) {
            continue;
         }
         if (communicationErrors.contains(statusEntry.getKey())) {
            status.updateOnCommunicationFailure(backupResponse.getSendTimeMillis());
            log.tracef("OfflineStatus updated %s", status);
         } else if (!status.isOffline()) {
            status.reset();
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
      //we have a 2PC: we didn't backup the 1PC stuff during prepare, we need to do it now.
      BackupResponse onePcResponse = sendTo1PCBackups(command);
      List<XSiteBackup> xSiteBackups = calculateBackupInfo(BackupFilter.KEEP_2PC_ONLY);
      BackupResponse twoPcResponse = backupCommand(command, xSiteBackups);
      return new AggregateBackupResponse(onePcResponse, twoPcResponse);
   }

   @Override
   public BackupResponse backupRollback(RollbackCommand command) throws Exception {
      List<XSiteBackup> xSiteBackups = calculateBackupInfo(BackupFilter.KEEP_2PC_ONLY);
      log.tracef("Backing up rollback command to: %s", xSiteBackups);
      return backupCommand(command, xSiteBackups);
   }


   @Override
   public BringSiteOnlineResponse bringSiteOnline(String siteName) {
      if (!config.sites().hasInUseBackup(siteName)) {
         log.tryingToBringOnlineNonexistentSite(siteName);
         return BringSiteOnlineResponse.NO_SUCH_SITE;
      } else {
         OfflineStatus offline = offlineStatus.get(siteName);
         boolean broughtOnline = offline.bringOnline();
         return broughtOnline ? BringSiteOnlineResponse.BROUGHT_ONLINE : BringSiteOnlineResponse.ALREADY_ONLINE;
      }
   }

   @Override
   public TakeSiteOfflineResponse takeSiteOffline(String siteName) {
      if (!config.sites().hasInUseBackup(siteName)) {
         return TakeSiteOfflineResponse.NO_SUCH_SITE;
      } else {
         OfflineStatus offline = offlineStatus.get(siteName);
         return offline.forceOffline() ? TakeSiteOfflineResponse.TAKEN_OFFLINE : TakeSiteOfflineResponse.ALREADY_OFFLINE;
      }
   }

   private BackupResponse backupCommand(VisitableCommand command, List<XSiteBackup> xSiteBackups) throws Exception {
      return transport.backupRemotely(xSiteBackups, commandsFactory.buildSingleXSiteRpcCommand(command));
   }

   private BackupResponse sendTo1PCBackups(CommitCommand command) throws Exception {
      final LocalTransaction localTx = txTable.getLocalTransaction(command.getGlobalTransaction());
      List<WriteCommand> modifications = filterModifications(localTx.getModifications());
      if (modifications.isEmpty()) {
         return EMPTY_RESPONSE;
      }
      List<XSiteBackup> backups = calculateBackupInfo(BackupFilter.KEEP_1PC_ONLY);
      PrepareCommand prepare = commandsFactory.buildPrepareCommand(command.getGlobalTransaction(),
                                                                   modifications, true);
      return backupCommand(prepare, backups);
   }

   private void processFailedResponses(BackupResponse backupResponse, VisitableCommand command, Transaction transaction) throws Throwable {
      SitesConfiguration sitesConfiguration = config.sites();
      Map<String, Throwable> failures = backupResponse.getFailedBackups();
      BackupFailureException backupException = null;
      for (Map.Entry<String, Throwable> failure : failures.entrySet()) {
         BackupFailurePolicy policy = sitesConfiguration.getFailurePolicy(failure.getKey());
         if (policy == BackupFailurePolicy.CUSTOM) {
            CustomFailurePolicy customFailurePolicy = siteFailurePolicy.get(failure.getKey());
            command.acceptVisitor(null, new CustomBackupPolicyInvoker(failure.getKey(), customFailurePolicy, transaction));
         }
         if (policy == BackupFailurePolicy.WARN) {
            log.warnXsiteBackupFailed(cacheName, failure.getKey(), failure.getValue());
         } else if (policy == BackupFailurePolicy.FAIL) {
            if (backupException == null)
               backupException = new BackupFailureException(cacheName);
            backupException.addFailure(failure.getKey(), failure.getValue());
         }
      }
      if (backupException != null)
         throw backupException;
   }

   private List<XSiteBackup> calculateBackupInfo(BackupFilter backupFilter) {
      List<XSiteBackup> backupInfo = new ArrayList<XSiteBackup>(2);
      SitesConfiguration sites = config.sites();
      for (BackupConfiguration bc : sites.enabledBackups()) {
         if (bc.site().equals(localSiteName)) {
            log.cacheBackupsDataToSameSite(localSiteName);
            continue;
         }
         boolean isSync = bc.strategy() == BackupConfiguration.BackupStrategy.SYNC;
         if (backupFilter == BackupFilter.KEEP_1PC_ONLY) {
            if (isSync && bc.isTwoPhaseCommit())
               continue;
         }

         if (backupFilter == BackupFilter.KEEP_2PC_ONLY) {
            if (!isSync || (!bc.isTwoPhaseCommit()))
               continue;
         }

         if (isOffline(bc.site())) {
            log.tracef("The site '%s' is offline, not backing up information to it", bc.site());
            continue;
         }
         XSiteBackup bi = new XSiteBackup(bc.site(), isSync, bc.replicationTimeout());
         backupInfo.add(bi);
      }
      return backupInfo;
   }

   private boolean isOffline(String site) {
      OfflineStatus offline = offlineStatus.get(site);
      return offline != null && offline.isOffline();
   }

   private List<WriteCommand> filterModifications(WriteCommand[] modifications) {
      if (modifications == null || modifications.length == 0) {
         return Collections.emptyList();
      }
      return filterModifications(Arrays.asList(modifications));
   }

   private List<WriteCommand> filterModifications(Collection<WriteCommand> modifications) {
      if (modifications == null || modifications.isEmpty()) {
         return Collections.emptyList();
      }
      List<WriteCommand> filtered = new ArrayList<WriteCommand>(modifications.size());
      for (WriteCommand writeCommand : modifications) {
         if (!writeCommand.isSuccessful()) {
            continue;
         }
         WriteCommand filteredCommand = writeCommand;
         if (writeCommand instanceof PutKeyValueCommand && ((PutKeyValueCommand) writeCommand).isPutIfAbsent()) {
            filteredCommand = commandsFactory.buildPutKeyValueCommand(((PutKeyValueCommand) writeCommand).getKey(),
                                                                      ((PutKeyValueCommand) writeCommand).getValue(),
                                                                      writeCommand.getMetadata(),
                                                                      writeCommand.getFlags());
         } else if (writeCommand instanceof ReplaceCommand) {
            filteredCommand = commandsFactory.buildPutKeyValueCommand(((ReplaceCommand) writeCommand).getKey(),
                                                                      ((ReplaceCommand) writeCommand).getNewValue(),
                                                                      writeCommand.getMetadata(),
                                                                      writeCommand.getFlags());
         } else if (writeCommand instanceof RemoveCommand && writeCommand.isConditional()) {
            filteredCommand = commandsFactory.buildRemoveCommand(((RemoveCommand) writeCommand).getKey(), null,
                                                                 writeCommand.getFlags());
         }
         filtered.add(filteredCommand);
      }
      return filtered;
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

   public OfflineStatus getOfflineStatus(String site) {
      return offlineStatus.get(site);
   }

   @Override
   public Map<String, Boolean> status() {
      Map<String, Boolean> result = new HashMap<String, Boolean>(offlineStatus.size());
      for (Map.Entry<String, OfflineStatus> os : offlineStatus.entrySet()) {
         result.put(os.getKey(), !os.getValue().isOffline());
      }
      return result;
   }

   private static class EmptyBackupResponse implements BackupResponse {

      @Override
      public void waitForBackupToFinish() throws Exception {
         //no-op, nothing was sent
      }

      @Override
      public Map<String, Throwable> getFailedBackups() {
         //no-op, nothing was sent
         return Collections.emptyMap();
      }

      @Override
      public Set<String> getCommunicationErrors() {
         //no-op, nothing was sent
         return Collections.emptySet();
      }

      @Override
      public long getSendTimeMillis() {
         //no-op, this should never be invoked
         return 0;
      }

      @Override
      public boolean isEmpty() {
         return true;
      }
   }
}
