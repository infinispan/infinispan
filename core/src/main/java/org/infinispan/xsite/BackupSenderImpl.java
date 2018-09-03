package org.infinispan.xsite;

import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.transaction.Transaction;

import org.infinispan.Cache;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
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
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.SitesConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.remoting.transport.AggregateBackupResponse;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;
import org.infinispan.xsite.notification.SiteStatusListener;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class BackupSenderImpl implements BackupSender {

   private static Log log = LogFactory.getLog(BackupSenderImpl.class);
   private static final BackupResponse EMPTY_RESPONSE = new EmptyBackupResponse();

   @Inject private ComponentRef<Cache> cache;
   @Inject private Transport transport;
   @Inject private Configuration config;
   @Inject private TransactionTable txTable;
   @Inject private TimeService timeService;
   @Inject private CommandsFactory commandsFactory;
   @Inject private EventLogManager eventLogManager;
   @Inject private GlobalConfiguration globalConfig;
   @Inject private KeyPartitioner keyPartitioner;

   private final Map<String, CustomFailurePolicy> siteFailurePolicy = new HashMap<>();
   private final ConcurrentMap<String, OfflineStatus> offlineStatus = CollectionFactory.makeConcurrentMap();
   private final String localSiteName;
   private String cacheName;

   private enum BackupFilter {KEEP_1PC_ONLY, KEEP_2PC_ONLY, KEEP_ALL}

   public BackupSenderImpl(String localSiteName) {
      this.localSiteName = localSiteName;
   }

   @Start
   public void start() {
      this.cacheName = cache.wired().getName();
      for (BackupConfiguration bc : config.sites().enabledBackups()) {
         final String siteName = bc.site();
         if (bc.backupFailurePolicy() == BackupFailurePolicy.CUSTOM) {
            String backupPolicy = bc.failurePolicyClass();
            if (backupPolicy == null) {
               throw new IllegalStateException("Backup policy class missing for custom failure policy!");
            }
            CustomFailurePolicy instance = Util.getInstance(backupPolicy, globalConfig.classLoader());
            instance.init(cache.wired());
            siteFailurePolicy.put(bc.site(), instance);
         }
         OfflineStatus offline = new OfflineStatus(bc.takeOffline(), timeService,
                                                   new SiteStatusListener() {
                                                      @Override
                                                      public void siteOnline() {
                                                         BackupSenderImpl.this.siteOnline(siteName);
                                                      }

                                                      @Override
                                                      public void siteOffline() {
                                                         BackupSenderImpl.this.siteOffline(siteName);
                                                      }
                                                   });
         offlineStatus.put(siteName, offline);
      }
   }

   @Override
   public BackupResponse backupPrepare(PrepareCommand command, AbstractCacheTransaction cacheTransaction) throws Exception {
      List<WriteCommand> modifications = filterModifications(command.getModifications(), cacheTransaction.getLookedUpEntries());
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
      List<WriteCommand> modifications = filterModifications(localTx.getModifications(), localTx.getLookedUpEntries());
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
      List<XSiteBackup> backupInfo = new ArrayList<>(2);
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

   private List<WriteCommand> filterModifications(WriteCommand[] modifications, Map<Object, CacheEntry> lookedUpEntries) {
      if (modifications == null || modifications.length == 0) {
         return Collections.emptyList();
      }
      return filterModifications(Arrays.asList(modifications), lookedUpEntries);
   }

   private List<WriteCommand> filterModifications(List<WriteCommand> modifications, Map<Object, CacheEntry> lookedUpEntries) {
      if (modifications == null || modifications.isEmpty()) {
         return Collections.emptyList();
      }
      List<WriteCommand> filtered = new ArrayList<>(modifications.size());
      Set<Object> filteredKeys = new HashSet<>(modifications.size());
      // Note: the result of replication of transaction with flagged operations may be actually different.
      // We use last-flag-bit-set wins strategy.
      // All we can do is to assume that if the user plays with unsafe flags he won't modify the entry once
      // in a replicable and another time in a non-replicable way
      for (ListIterator<WriteCommand> it = modifications.listIterator(modifications.size()); it.hasPrevious(); ) {
         WriteCommand writeCommand = it.previous();
         if (!writeCommand.isSuccessful() || writeCommand.hasAnyFlag(FlagBitSets.SKIP_XSITE_BACKUP)) {
            continue;
         }
         // Note: ClearCommand should be replicated out of transaction
         for (Object key : writeCommand.getAffectedKeys()) {
            if (filteredKeys.contains(key)) {
               continue;
            }
            CacheEntry entry = lookedUpEntries.get(key);
            if (entry == null) {
               // Functional commands should always fetch the remote value to originator if xsite is enabled.
               throw new IllegalStateException();
            }
            WriteCommand replicatedCommand;
            if (entry.isRemoved()) {
               replicatedCommand = commandsFactory.buildRemoveCommand(key, null, keyPartitioner.getSegment(key),
                     writeCommand.getFlagsBitSet());
            } else if (entry.isChanged()) {
               replicatedCommand = commandsFactory.buildPutKeyValueCommand(key, entry.getValue(),
                     keyPartitioner.getSegment(key), entry.getMetadata(), writeCommand.getFlagsBitSet());
            } else {
               continue;
            }
            filtered.add(replicatedCommand);
            filteredKeys.add(key);
         }
      }
      return filtered;
   }

   private void siteOnline(String siteName) {
      getEventLogger().info(EventLogCategory.CLUSTER, MESSAGES.siteOnline(siteName));
   }

   private void siteOffline(String siteName) {
      getEventLogger().info(EventLogCategory.CLUSTER, MESSAGES.siteOffline(siteName));
   }

   private EventLogger getEventLogger() {
      return eventLogManager.getEventLogger().context(cacheName).scope(transport.getAddress());
   }

   private static final class CustomBackupPolicyInvoker extends AbstractVisitor {

      private final String site;
      private final CustomFailurePolicy<Object, Object> failurePolicy;
      private final Transaction tx;

      public CustomBackupPolicyInvoker(String site, CustomFailurePolicy failurePolicy, Transaction tx) {
         this.site = site;
         //noinspection unchecked
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
      public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
         failurePolicy.handleComputeFailure(site, command.getKey(), command.getRemappingBiFunction(), command.isComputeIfPresent());
         return null;
      }

      @Override
      public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
         failurePolicy.handleComputeIfAbsentFailure(site, command.getKey(), command.getMappingFunction());
         return null;
      }

      @Override
      public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
         failurePolicy.handleWriteOnlyKeyFailure(site, command.getKey());
         return null;
      }

      @Override
      public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
         failurePolicy.handleReadWriteKeyValueFailure(site, command.getKey());
         return null;
      }

      @Override
      public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
         failurePolicy.handleReadWriteKeyFailure(site, command.getKey());
         return null;
      }

      @Override
      public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
         failurePolicy.handleWriteOnlyManyEntriesFailure(site, command.getArguments());
         return null;
      }

      @Override
      public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
         failurePolicy.handleWriteOnlyKeyValueFailure(site, command.getKey());
         return null;
      }

      @Override
      public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
         failurePolicy.handleWriteOnlyManyFailure(site, command.getAffectedKeys());
         return null;
      }

      @Override
      public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
         failurePolicy.handleReadWriteManyFailure(site, command.getAffectedKeys());
         return null;
      }

      @Override
      public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
         failurePolicy.handleReadWriteManyEntriesFailure(site, command.getArguments());
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
      Map<String, Boolean> result = new HashMap<>(offlineStatus.size());
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
