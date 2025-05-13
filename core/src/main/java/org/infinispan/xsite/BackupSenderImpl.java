package org.infinispan.xsite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.CustomFailurePolicy;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.SyncInvocationStage;
import org.infinispan.interceptors.impl.SimpleAsyncInvocationStage;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.xsite.commands.remote.XSiteCacheRequest;
import org.infinispan.xsite.status.SiteState;
import org.infinispan.xsite.status.TakeOfflineManager;

import jakarta.transaction.Transaction;
import net.jcip.annotations.GuardedBy;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Scope(Scopes.NAMED_CACHE)
public class BackupSenderImpl implements BackupSender {

   private static final Log log = LogFactory.getLog(BackupSenderImpl.class);

   @Inject ComponentRef<Cache<Object,Object>> cache;
   @Inject RpcManager rpcManager;
   @Inject Configuration config;
   @Inject TransactionTable txTable;
   @Inject TimeService timeService;
   @Inject CommandsFactory commandsFactory;
   @Inject EventLogManager eventLogManager;
   @Inject GlobalConfiguration globalConfig;
   @Inject KeyPartitioner keyPartitioner;
   @Inject TakeOfflineManager takeOfflineManager;

   private final Map<String, CustomFailurePolicy<Object,Object>> siteFailurePolicy = new HashMap<>();
   private String localSiteName;
   private String cacheName;

   private enum BackupFilter {KEEP_1PC_ONLY, KEEP_2PC_ONLY, KEEP_ALL}

   public BackupSenderImpl() {
   }

   @Start
   public void start() {
      Transport transport = rpcManager.getTransport();
      transport.checkCrossSiteAvailable();
      this.cacheName = cache.wired().getName();
      this.localSiteName = transport.localSiteName();
      config.sites().syncBackupsStream()
            .filter(bc ->  bc.backupFailurePolicy() == BackupFailurePolicy.CUSTOM)
            .forEach(bc -> {
               String backupPolicy = bc.failurePolicyClass();
               if (backupPolicy == null) {
                  throw new IllegalStateException("Backup policy class missing for custom failure policy!");
               }
               CustomFailurePolicy<Object, Object> instance = Util.getInstance(backupPolicy, globalConfig.classLoader());
               instance.init(cache.wired());
               siteFailurePolicy.put(bc.site(), instance);
            });
   }

   @Override
   public InvocationStage backupPrepare(PrepareCommand command, AbstractCacheTransaction cacheTransaction,
         Transaction transaction) {
      List<WriteCommand> modifications = filterModifications(command.getModifications(),
            cacheTransaction.getLookedUpEntries());
      if (modifications.isEmpty()) {
         return SyncInvocationStage.completedNullStage();
      }
      PrepareCommand prepare = commandsFactory.buildPrepareCommand(command.getGlobalTransaction(), modifications,
            command.isOnePhaseCommit());
      //if we run a 2PC then filter out 1PC prepare backup calls as they will happen during the local commit phase.
      BackupFilter filter = !prepare.isOnePhaseCommit() ? BackupFilter.KEEP_2PC_ONLY : BackupFilter.KEEP_ALL;
      List<XSiteBackup> backups = calculateBackupInfo(filter);
      if (backups.isEmpty()) {
         return SyncInvocationStage.completedNullStage();
      }
      return backupCommand(prepare, command, backups, transaction);
   }

   @Override
   public InvocationStage backupWrite(WriteCommand command, WriteCommand originalCommand) {
      List<XSiteBackup> xSiteBackups = calculateBackupInfo(BackupFilter.KEEP_ALL);
      return backupCommand(command, originalCommand, xSiteBackups, null);
   }

   @Override
   public InvocationStage backupClear(ClearCommand command) {
      List<XSiteBackup> xSiteBackups = calculateBackupInfo(BackupFilter.KEEP_ALL);
      return backupCommand(command, command, xSiteBackups, null);
   }

   @Override
   public InvocationStage backupCommit(CommitCommand command, Transaction transaction) {
      ResponseAggregator aggregator = new ResponseAggregator(command, transaction);

      //we have a 2PC: we didn't backup the 1PC stuff during prepare, we need to do it now.
      sendTo1PCBackups(command, aggregator);
      sendTo2PCBackups(command, aggregator);
      return aggregator.freeze();
   }

   @Override
   public InvocationStage backupRollback(RollbackCommand command, Transaction transaction) {
      List<XSiteBackup> xSiteBackups = calculateBackupInfo(BackupFilter.KEEP_2PC_ONLY);
      return backupCommand(command, command, xSiteBackups, transaction);
   }

   public CustomFailurePolicy<Object, Object> getCustomFailurePolicy(String site) {
      return siteFailurePolicy.get(site);
   }

   private InvocationStage backupCommand(VisitableCommand command, VisitableCommand originalCommand,
         List<XSiteBackup> xSiteBackups, Transaction transaction) {
      XSiteCacheRequest<Object> xsiteCommand = commandsFactory.buildSingleXSiteRpcCommand(command);
      ResponseAggregator aggregator = new ResponseAggregator(originalCommand, transaction);
      sendTo(xsiteCommand, xSiteBackups, aggregator);
      return aggregator.freeze();
   }

   private void sendTo(XSiteCacheRequest<Object> command, Collection<XSiteBackup> xSiteBackups,
                       ResponseAggregator aggregator) {
      for (XSiteBackup backup : xSiteBackups) {
         XSiteResponse<Object> cs = rpcManager.invokeXSite(backup, command);
         takeOfflineManager.registerRequest(cs);
         aggregator.addResponse(backup, cs);
      }
   }

   private void sendTo1PCBackups(CommitCommand command, ResponseAggregator aggregator) {
      final LocalTransaction localTx = txTable.getLocalTransaction(command.getGlobalTransaction());
      List<WriteCommand> modifications = filterModifications(localTx.getModifications(), localTx.getLookedUpEntries());
      if (modifications.isEmpty()) {
         return; //nothing to send
      }
      List<XSiteBackup> xSiteBackups = calculateBackupInfo(BackupFilter.KEEP_1PC_ONLY);
      if (xSiteBackups.isEmpty()) {
         return; //avoid creating garbage
      }
      PrepareCommand prepare = commandsFactory.buildPrepareCommand(command.getGlobalTransaction(),
                                                                   modifications, true);
      XSiteCacheRequest<Object> xsiteCommand = commandsFactory.buildSingleXSiteRpcCommand(prepare);
      sendTo(xsiteCommand, xSiteBackups, aggregator);
   }

   private void sendTo2PCBackups(CommitCommand command, ResponseAggregator aggregator) {
      List<XSiteBackup> xSiteBackups = calculateBackupInfo(BackupFilter.KEEP_2PC_ONLY);
      if (xSiteBackups.isEmpty()) {
         return; //avoid creating garbage
      }
      XSiteCacheRequest<Object> xsiteCommand = commandsFactory.buildSingleXSiteRpcCommand(command);
      sendTo(xsiteCommand, xSiteBackups, aggregator);
   }

   private List<XSiteBackup> calculateBackupInfo(BackupFilter backupFilter) {
      List<XSiteBackup> backupInfo = new ArrayList<>(2);
      Iterator<BackupConfiguration> iterator = config.sites().syncBackupsStream().iterator();
      while (iterator.hasNext()){
         BackupConfiguration bc = iterator.next();
         if (bc.site().equals(localSiteName)) {
            log.cacheBackupsDataToSameSite(localSiteName);
            continue;
         }
         boolean is2PC = bc.isTwoPhaseCommit();
         if (backupFilter == BackupFilter.KEEP_1PC_ONLY && is2PC) {
            continue;
         }

         if (backupFilter == BackupFilter.KEEP_2PC_ONLY && !is2PC) {
            continue;
         }
         if (takeOfflineManager.getSiteState(bc.site()) == SiteState.OFFLINE) {
            log.tracef("The site '%s' is offline, not backing up information to it", bc.site());
            continue;
         }
         XSiteBackup bi = new XSiteBackup(bc.site(), true, bc.replicationTimeout());
         backupInfo.add(bi);
      }
      return backupInfo;
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
         if (!writeCommand.shouldReplicate(null, true) || writeCommand.hasAnyFlag(FlagBitSets.SKIP_XSITE_BACKUP)) {
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

   private class ResponseAggregator extends CompletableFuture<Void> implements XSiteResponse.XSiteResponseCompleted {

      private final VisitableCommand command;
      private final Transaction transaction;
      private final AtomicInteger counter;
      @GuardedBy("this")
      private BackupFailureException exception;
      private volatile boolean frozen;

      private ResponseAggregator(VisitableCommand command, Transaction transaction) {
         this.command = command;
         this.transaction = transaction;
         this.counter = new AtomicInteger();
      }

      @Override
      public void onCompleted(XSiteBackup backup, long sendTimeNanos, long durationNanos, Throwable throwable) {
         if (log.isTraceEnabled()) {
            log.tracef("Backup response from site %s completed for command %s. throwable=%s", command, backup,
                  throwable);
         }

         if (backup.isSync()) {
            if (throwable != null) {
               handleException(backup.getSiteName(), throwable);
            }
            if (counter.decrementAndGet() == 0 && frozen) {
               onRequestCompleted();
            }
         }
      }

      void addResponse(XSiteBackup backup, XSiteResponse<Object> response) {
         assert !frozen;
         response.whenCompleted(this);
         if (backup.isSync()) {
            counter.incrementAndGet();
         }
      }

      InvocationStage freeze() {
         frozen = true;
         if (counter.get() == 0) {
            onRequestCompleted();
         }
         return new SimpleAsyncInvocationStage(this);
      }

      void handleException(String siteName, Throwable throwable) {
         switch (config.sites().getFailurePolicy(siteName)) {
            case FAIL:
               addException(siteName, throwable);
               break;
            case CUSTOM:
               try {
                  command.acceptVisitor(null, new CustomBackupPolicyInvoker(siteName, getCustomFailurePolicy(siteName), transaction));
               } catch (Throwable t) {
                  addException(siteName, t);
               }
               break;
            case WARN:
               log.warnXsiteBackupFailed(cacheName, siteName, throwable);
               //fallthrough
            default:
               break;
         }
      }

      synchronized void addException(String siteName, Throwable throwable) {
         if (exception == null) {
            exception = new BackupFailureException(cacheName);
         }
         exception.addSuppressed(new BackupFailureException(siteName, throwable));
      }

      private synchronized void onRequestCompleted() {
         if (exception != null) {
            completeExceptionally(exception);
         } else {
            complete(null);
         }
      }
   }

   private static final class CustomBackupPolicyInvoker extends AbstractVisitor {

      private final String site;
      private final CustomFailurePolicy<Object, Object> failurePolicy;
      private final Transaction tx;

      public CustomBackupPolicyInvoker(String site, CustomFailurePolicy<Object, Object> failurePolicy, Transaction tx) {
         this.site = site;
         this.failurePolicy = failurePolicy;
         this.tx = tx;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
         failurePolicy.handlePutFailure(site, command.getKey(), command.getValue(), command.isPutIfAbsent());
         return null;
      }

      @Override
      public Object visitIracPutKeyValueCommand(InvocationContext ctx, IracPutKeyValueCommand command) {
         //no-op
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
         failurePolicy.handleRemoveFailure(site, command.getKey(), command.getValue());
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
         failurePolicy.handleReplaceFailure(site, command.getKey(), command.getOldValue(), command.getNewValue());
         return null;
      }

      @Override
      public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) {
         failurePolicy.handleComputeFailure(site, command.getKey(), command.getRemappingBiFunction(), command.isComputeIfPresent());
         return null;
      }

      @Override
      public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) {
         failurePolicy.handleComputeIfAbsentFailure(site, command.getKey(), command.getMappingFunction());
         return null;
      }

      @Override
      public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) {
         failurePolicy.handleWriteOnlyKeyFailure(site, command.getKey());
         return null;
      }

      @Override
      public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) {
         failurePolicy.handleReadWriteKeyValueFailure(site, command.getKey());
         return null;
      }

      @Override
      public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) {
         failurePolicy.handleReadWriteKeyFailure(site, command.getKey());
         return null;
      }

      @Override
      public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) {
         failurePolicy.handleWriteOnlyManyEntriesFailure(site, command.getArguments());
         return null;
      }

      @Override
      public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) {
         failurePolicy.handleWriteOnlyKeyValueFailure(site, command.getKey());
         return null;
      }

      @Override
      public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) {
         failurePolicy.handleWriteOnlyManyFailure(site, command.getAffectedKeys());
         return null;
      }

      @Override
      public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) {
         failurePolicy.handleReadWriteManyFailure(site, command.getAffectedKeys());
         return null;
      }

      @Override
      public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) {
         failurePolicy.handleReadWriteManyEntriesFailure(site, command.getArguments());
         return null;
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) {
         failurePolicy.handleClearFailure(site);
         return null;
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
         failurePolicy.handlePutAllFailure(site, command.getMap());
         return null;
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
         failurePolicy.handlePrepareFailure(site, tx);
         return null;
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) {
         failurePolicy.handleRollbackFailure(site, tx);
         return null;
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
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
