package org.infinispan.interceptors.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.TxReadOnlyKeyCommand;
import org.infinispan.commands.functional.TxReadOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.group.impl.GroupFilter;
import org.infinispan.distribution.group.impl.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.CompositeKeyFilter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationFinallyAction;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.responses.Response;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteStateConsumer;

/**
 * Interceptor in charge with wrapping entries and add them in caller's context.
 *
 * @see EntryFactory for overview of entry wrapping.
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 * @since 9.0
 */
public class EntryWrappingInterceptor extends DDAsyncInterceptor {
   private EntryFactory entryFactory;
   private DataContainer<Object, Object> dataContainer;
   protected ClusteringDependentLogic cdl;
   private VersionGenerator versionGenerator;
   private DistributionManager distributionManager;
   private final EntryWrappingVisitor entryWrappingVisitor = new EntryWrappingVisitor();
   private boolean isInvalidation;
   private boolean isSync;
   private StateConsumer stateConsumer;       // optional
   private StateTransferLock stateTransferLock;
   private XSiteStateConsumer xSiteStateConsumer;
   private GroupManager groupManager;
   private CacheNotifier notifier;
   private StateTransferManager stateTransferManager;
   private boolean useRepeatableRead;
   private boolean isVersioned;

   private static final Log log = LogFactory.getLog(EntryWrappingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final long EVICT_FLAGS_BITSET =
         FlagBitSets.SKIP_OWNERSHIP_CHECK | FlagBitSets.CACHE_MODE_LOCAL;
   private boolean totalOrder;

   private final InvocationSuccessAction dataReadReturnHandler = (rCtx, rCommand, rv) -> {
      AbstractDataCommand dataCommand = (AbstractDataCommand) rCommand;

      if (rCtx.isInTxScope() && useRepeatableRead) {
         // The entry must be in the context
         CacheEntry cacheEntry = rCtx.lookupEntry(dataCommand.getKey());
         cacheEntry.setSkipLookup(true);
         if (isVersioned && ((MVCCEntry) cacheEntry).isRead()) {
            addVersionRead((TxInvocationContext) rCtx, cacheEntry, dataCommand.getKey());
         }
      }

      // Entry visit notifications used to happen in the CallInterceptor
      // We do it at the end to avoid adding another try/finally block around the notifications
      if (rv != null && !(rv instanceof Response)) {
         Object value = dataCommand instanceof GetCacheEntryCommand ? ((CacheEntry) rv).getValue() : rv;
         notifier.notifyCacheEntryVisited(dataCommand.getKey(), value, true, rCtx, dataCommand);
         notifier.notifyCacheEntryVisited(dataCommand.getKey(), value, false, rCtx, dataCommand);
      }
   };

   private final InvocationSuccessAction commitEntriesSuccessHandler = (rCtx, rCommand, rv) -> commitContextEntries(rCtx, null);

   private final InvocationFinallyAction
         commitEntriesFinallyHandler = (rCtx, rCommand, rv, t) -> commitContextEntries(rCtx, null);

   private final InvocationSuccessFunction prepareHandler = this::prepareHandler;

   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(EntryFactory entryFactory, DataContainer<Object, Object> dataContainer, ClusteringDependentLogic cdl,
                    StateConsumer stateConsumer, StateTransferLock stateTransferLock,
                    XSiteStateConsumer xSiteStateConsumer, GroupManager groupManager, CacheNotifier notifier,
                    StateTransferManager stateTransferManager, VersionGenerator versionGenerator, DistributionManager distributionManager) {
      this.entryFactory = entryFactory;
      this.dataContainer = dataContainer;
      this.cdl = cdl;
      this.stateConsumer = stateConsumer;
      this.stateTransferLock = stateTransferLock;
      this.xSiteStateConsumer = xSiteStateConsumer;
      this.groupManager = groupManager;
      this.notifier = notifier;
      this.stateTransferManager = stateTransferManager;
      this.versionGenerator = versionGenerator;
      this.distributionManager = distributionManager;
   }

   @Start
   public void start() {
      isInvalidation = cacheConfiguration.clustering().cacheMode().isInvalidation();
      isSync = cacheConfiguration.clustering().cacheMode().isSynchronous();
      // isolation level makes no sense without transactions
      useRepeatableRead = cacheConfiguration.transaction().transactionMode().isTransactional()
            && cacheConfiguration.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ;
      isVersioned = Configurations.isTxVersioned(cacheConfiguration);
      totalOrder = cacheConfiguration.transaction().transactionProtocol().isTotalOrder();
   }

   private boolean ignoreOwnership(FlagAffectedCommand command) {
      return stateTransferManager == null || command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_OWNERSHIP_CHECK);
   }

   private boolean canRead(Object key) {
      return distributionManager.getCacheTopology().isReadOwner(key);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return wrapEntriesForPrepareAndApply(ctx, command, prepareHandler);
   }

   private Object prepareHandler(InvocationContext ctx, VisitableCommand command, Object rv) {
      if (shouldCommitDuringPrepare((PrepareCommand) command, (TxInvocationContext) ctx)) {
         return invokeNextThenAccept(ctx, command, commitEntriesSuccessHandler);
      } else {
         return invokeNext(ctx, command);
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNextAndFinally(ctx, command, commitEntriesFinallyHandler);
   }

   @Override
   public final Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command)
         throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   @Override
   public final Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   private Object visitDataReadCommand(InvocationContext ctx, AbstractDataCommand command) {
      final Object key = command.getKey();
      entryFactory.wrapEntryForReading(ctx, key, ignoreOwnership(command) || canRead(key));
      return invokeNextThenAccept(ctx, command, dataReadReturnHandler);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      boolean ignoreOwnership = ignoreOwnership(command);
      for (Object key : command.getKeys()) {
         entryFactory.wrapEntryForReading(ctx, key, ignoreOwnership || canRead(key));
      }
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         GetAllCommand getAllCommand = (GetAllCommand) rCommand;
         if (useRepeatableRead) {
            for (Object key : getAllCommand.getKeys()) {
               CacheEntry cacheEntry = rCtx.lookupEntry(key);
               if (trace && cacheEntry == null) log.tracef(t, "Missing entry for " + key);
               cacheEntry.setSkipLookup(true);
            }
         }

         // Entry visit notifications used to happen in the CallInterceptor
         // instanceof check excludes the case when the command returns UnsuccessfulResponse
         if (t == null && rv instanceof Map) {
            log.tracef("Notifying getAll? %s; result %s", !command.hasAnyFlag(FlagBitSets.SKIP_LISTENER_NOTIFICATION), rv);
            Map<Object, Object> map = (Map<Object, Object>) rv;
            // TODO: it would be nice to know if a listener was registered for this and
            // not do the full iteration if there was no visitor listener registered
            if (!command.hasAnyFlag(FlagBitSets.SKIP_LISTENER_NOTIFICATION)) {
               for (Map.Entry<Object, Object> entry : map.entrySet()) {
                  Object value = entry.getValue();
                  if (value != null) {
                     value = command.isReturnEntries() ? ((CacheEntry) value).getValue() : entry.getValue();
                     notifier.notifyCacheEntryVisited(entry.getKey(), value, true, rCtx, getAllCommand);
                     notifier.notifyCacheEntryVisited(entry.getKey(), value, false, rCtx, getAllCommand);
                  }
               }
            }
         }
      });
   }

   @Override
   public final Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command)
         throws Throwable {
      if (command.getKeys() != null) {
         for (Object key : command.getKeys()) {
            // TODO: move this to distribution interceptors?
            // we need to try to wrap the entry to get it removed
            // for the removal itself, wrapping null would suffice, but listeners need previous value
            entryFactory.wrapEntryForWriting(ctx, key, true, false);
         }
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   @Override
   public final Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         // If we are committing a ClearCommand now then no keys should be written by state transfer from
         // now on until current rebalance ends.
         if (stateConsumer != null) {
            stateConsumer.stopApplyingState(((ClearCommand) rCommand).getTopologyId());
         }
         if (xSiteStateConsumer != null) {
            xSiteStateConsumer.endStateTransfer(null);
         }

         if (!rCtx.isInTxScope()) {
            ClearCommand clearCommand = (ClearCommand) rCommand;
            applyChanges(rCtx, clearCommand);
         }

         if (trace)
            log.tracef("The return value is %s", rv);
      });
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command)
         throws Throwable {
      for (Object key : command.getKeys()) {
         // TODO: move to distribution interceptors?
         // we need to try to wrap the entry to get it removed
         // for the removal itself, wrapping null would suffice, but listeners need previous value
         entryFactory.wrapEntryForWriting(ctx, key, false, false);
         if (trace)
           log.tracef("Entry to be removed: %s", toStr(key));
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   @Override
   public final Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command);
   }

   private void wrapEntryIfNeeded(InvocationContext ctx, AbstractDataWriteCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getKey());
      }
      entryFactory.wrapEntryForWriting(ctx, command.getKey(), ignoreOwnership(command) || canRead(command.getKey()), command.loadType() != VisitableCommand.LoadType.DONT_LOAD);
   }

   private void removeFromContextOnRetry(InvocationContext ctx, Object key) {
      // When originator is a backup and it becomes primary (and we retry the command), the context already
      // contains the value before the command started to be executed. However, another modification could happen
      // after this node became an owner, so we have to force a reload.
      // With repeatable reads, we cannot just remove the entry from context; instead of we will rely
      // on the write skew check to do the reload & comparison in the end.
      // With pessimistic locking, there's no WSC but as we have the entry locked, there shouldn't be any
      // modification concurrent to the retried operation, therefore we don't have to deal with this problem.
      if (useRepeatableRead) {
         MVCCEntry entry = (MVCCEntry) ctx.lookupEntry(key);
         if (trace) {
            log.tracef("This is a retry - resetting previous value in entry ", entry);
         }
         entry.resetCurrentValue();
      } else {
         if (trace) {
            log.tracef("This is a retry - removing looked up entry " + ctx.lookupEntry(key));
         }
         ctx.removeLookedUpEntry(key);
      }
   }

   private void removeFromContextOnRetry(InvocationContext ctx, Collection<?> keys) {
      if (useRepeatableRead) {
         if (trace) {
            log.tracef("This is a retry - resetting previous values for %s", keys);
         }
         for (Object key : keys) {
            MVCCEntry entry = (MVCCEntry) ctx.lookupEntry(key);
            entry.resetCurrentValue();
         }
      } else {
         ctx.removeLookedUpEntries(keys);
      }
   }

   @Override
   public final Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command);
   }

   @Override
   public final Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
         throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      boolean ignoreOwnership = ignoreOwnership(command);
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      for (Object key : command.getMap().keySet()) {
         // as listeners may need the value, we'll load the previous value
         entryFactory.wrapEntryForWriting(ctx, key, ignoreOwnership || canRead(key), command.loadType() != VisitableCommand.LoadType.DONT_LOAD);
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      command.setFlagsBitSet(EVICT_FLAGS_BITSET); //to force the wrapping
      return visitRemoveCommand(ctx, command);
   }

   @Override
   public Object visitGetKeysInGroupCommand(final InvocationContext ctx, GetKeysInGroupCommand command)
         throws Throwable {
      if (command.isGroupOwner()) {
         final KeyFilter<Object> keyFilter = new CompositeKeyFilter<>(new GroupFilter<>(command.getGroupName(), groupManager),
               new CollectionKeyFilter<>(ctx.getLookedUpEntries().keySet()));
         dataContainer.executeTask(keyFilter, (o, internalCacheEntry) -> {
            // Don't wrap tombstones into context; we want to be able to eventually read these values from
            // cache store and the filter in CacheLoaderInterceptor ignores keys already in context
            if (internalCacheEntry.getValue() != null) {
               synchronized (ctx) {
                  //the process can be made in multiple threads, so we need to synchronize in the context.
                  entryFactory.wrapExternalEntry(ctx, internalCacheEntry.getKey(), internalCacheEntry, true, false);
               }
            }
         });
      }
      // We don't make sure that all read entries have skipLookup here, since EntryFactory does that
      // for those we have really read, and there shouldn't be any null-read entries.
      if (ctx.isInTxScope() && useRepeatableRead) {
         return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
            TxInvocationContext txCtx = (TxInvocationContext) rCtx;
            for (Map.Entry<Object, CacheEntry> keyEntry : txCtx.getLookedUpEntries().entrySet()) {
               CacheEntry cacheEntry = keyEntry.getValue();
               cacheEntry.setSkipLookup(true);
               if (isVersioned && ((MVCCEntry) cacheEntry).isRead()) {
                  addVersionRead(txCtx, cacheEntry, keyEntry.getKey());
               }
            }
         });
      } else {
         return invokeNext(ctx, command);
      }
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      if (command instanceof TxReadOnlyKeyCommand) {
         // TxReadOnlyKeyCommand may apply some mutations on the entry in context so we need to always wrap it
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), ignoreOwnership(command) || canRead(command.getKey()), true);
      } else {
         entryFactory.wrapEntryForReading(ctx, command.getKey(), ignoreOwnership(command) || canRead(command.getKey()));
      }

      // Repeatable reads are not achievable with functional commands, as we don't store the value locally
      // and we don't "fix" it on the remote node; therefore, the value will be able to change and identity read
      // could return different values in the same transaction.
      // (Note: at this point TX mode is not implemented for functional commands anyway).
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      boolean ignoreOwnership = ignoreOwnership(command);
      if (command instanceof TxReadOnlyManyCommand) {
         // TxReadOnlyManyCommand may apply some mutations on the entry in context so we need to always wrap it
         for (Object key : command.getKeys()) {
            entryFactory.wrapEntryForWriting(ctx, key, ignoreOwnership(command) || canRead(key), true);
         }
      } else {
         for (Object key : command.getKeys()) {
            entryFactory.wrapEntryForReading(ctx, key, ignoreOwnership || canRead(key));
         }
      }
      // Repeatable reads are not achievable with functional commands, see visitReadOnlyKeyCommand
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command)
         throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command)
         throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx,
                                                  WriteOnlyManyEntriesCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      boolean ignoreOwnership = ignoreOwnership(command);
      for (Object key : command.getEntries().keySet()) {
         //the put map never reads the keys
         entryFactory.wrapEntryForWriting(ctx, key, ignoreOwnership || canRead(key), false);
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command)
         throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      boolean ignoreOwnership = ignoreOwnership(command);
      for (Object key : command.getAffectedKeys()) {
         entryFactory.wrapEntryForWriting(ctx, key, ignoreOwnership || canRead(key), false);
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command)
         throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command)
         throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      boolean ignoreOwnership = ignoreOwnership(command);
      for (Object key : command.getAffectedKeys()) {
         entryFactory.wrapEntryForWriting(ctx, key, ignoreOwnership || canRead(key), true);
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx,
                                                  ReadWriteManyEntriesCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      boolean ignoreOwnership = ignoreOwnership(command);
      for (Object key : command.getAffectedKeys()) {
         entryFactory.wrapEntryForWriting(ctx, key, ignoreOwnership || canRead(key), true);
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   protected final void commitContextEntries(InvocationContext ctx, FlagAffectedCommand command) {
      final Flag stateTransferFlag = FlagBitSets.extractStateTransferFlag(ctx, command);

      if (ctx instanceof SingleKeyNonTxInvocationContext) {
         SingleKeyNonTxInvocationContext singleKeyCtx = (SingleKeyNonTxInvocationContext) ctx;
         commitEntryIfNeeded(ctx, command,
                             singleKeyCtx.getCacheEntry(), stateTransferFlag);
      } else {
         Set<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
         Iterator<Map.Entry<Object, CacheEntry>> it = entries.iterator();
         final Log log = getLog();
         while (it.hasNext()) {
            Map.Entry<Object, CacheEntry> e = it.next();
            CacheEntry entry = e.getValue();
            if (!commitEntryIfNeeded(ctx, command, entry, stateTransferFlag)) {
               if (trace) {
                  if (entry == null)
                     log.tracef("Entry for key %s is null : not calling commitUpdate", toStr(e.getKey()));
                  else
                     log.tracef("Entry for key %s is not changed(%s): not calling commitUpdate", toStr(e.getKey()), entry);
               }
            }
         }
      }
   }

   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, FlagAffectedCommand command,
                                     Flag stateTransferFlag, boolean l1Invalidation) {
      cdl.commitEntry(entry, command, ctx, stateTransferFlag, l1Invalidation);
   }

   private void applyChanges(InvocationContext ctx, WriteCommand command) {
      stateTransferLock.acquireSharedTopologyLock();
      try {
         // We only retry non-tx write commands
         if (!isInvalidation) {
            // Can't perform the check during preload or if the cache isn't clustered
            boolean syncRpc = isSync && !command.hasAnyFlag(FlagBitSets.FORCE_ASYNCHRONOUS) ||
                  command.hasAnyFlag(FlagBitSets.FORCE_SYNCHRONOUS);
            if (command.isSuccessful() && stateConsumer != null && stateConsumer.getCacheTopology() != null) {
               int commandTopologyId = command.getTopologyId();
               int currentTopologyId = stateConsumer.getCacheTopology().getTopologyId();
               // TotalOrderStateTransferInterceptor doesn't set the topology id for PFERs.
               if (syncRpc && currentTopologyId != commandTopologyId && commandTopologyId != -1) {
                  // If we were the originator of a data command which we didn't own the key at the time means it
                  // was already committed, so there is no need to throw the OutdatedTopologyException
                  // This will happen if we submit a command to the primary owner and it responds and then a topology
                  // change happens before we get here
                  if (!ctx.isOriginLocal() || !(command instanceof DataCommand) ||
                            ctx.hasLockedKey(((DataCommand)command).getKey())) {
                     if (trace) log.tracef("Cache topology changed while the command was executing: expected %d, got %d",
                           commandTopologyId, currentTopologyId);
                     // This shouldn't be necessary, as we'll have a fresh command instance when retrying
                     command.setValueMatcher(command.getValueMatcher().matcherForRetry());
                     throw new OutdatedTopologyException("Cache topology changed while the command was executing: expected " +
                           commandTopologyId + ", got " + currentTopologyId);
                  }
               }
            }
         }

         commitContextEntries(ctx, command);
      } finally {
         stateTransferLock.releaseSharedTopologyLock();
      }
   }

   /**
    * Locks the value for the keys accessed by the command to avoid being override from a remote get.
    */
   protected Object setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(InvocationContext ctx, WriteCommand command) {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         WriteCommand writeCommand = (WriteCommand) rCommand;
         if (!rCtx.isInTxScope()) {
            applyChanges(rCtx, writeCommand);
            return;
         }

         if (trace)
            log.tracef("The return value is %s", toStr(rv));
         if (useRepeatableRead) {
            boolean addVersionRead = isVersioned && writeCommand.loadType() != VisitableCommand.LoadType.DONT_LOAD;
            TxInvocationContext txCtx = (TxInvocationContext) rCtx;
            for (Object key : writeCommand.getAffectedKeys()) {
               CacheEntry cacheEntry = rCtx.lookupEntry(key);
               if (cacheEntry != null) {
                  cacheEntry.setSkipLookup(true);
                  if (addVersionRead && ((MVCCEntry) cacheEntry).isRead()) {
                     addVersionRead(txCtx, cacheEntry, key);
                  }
                  ((MVCCEntry) cacheEntry).updatePreviousValue();
               }
            }
         }
      });
   }

   private void addVersionRead(TxInvocationContext rCtx, CacheEntry cacheEntry, Object key) {
      EntryVersion version;
      if (cacheEntry != null && cacheEntry.getMetadata() != null) {
         version = cacheEntry.getMetadata().version();
         if (trace) log.tracef("Adding version read %s for key %s", version, key);
      } else {
         version = versionGenerator.nonExistingVersion();
         if (trace) log.tracef("Adding non-existent version read for key %s", key);
      }
      rCtx.getCacheTransaction().addVersionRead(key, version);
   }

   /**
    * Locks the value for the keys accessed by the command to avoid being override from a remote get.
    */
   protected Object setSkipRemoteGetsAndInvokeNextForDataCommand(InvocationContext ctx,
                                                               DataWriteCommand command) {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
         if (!rCtx.isInTxScope()) {
            applyChanges(rCtx, dataWriteCommand);
            return;
         }

         if (trace)
            log.tracef("The return value is %s", rv);
         if (useRepeatableRead) {
            CacheEntry cacheEntry = rCtx.lookupEntry(dataWriteCommand.getKey());
            // The entry is not in context when the command's execution type does not contain origin
            if (cacheEntry != null) {
               cacheEntry.setSkipLookup(true);
               if (isVersioned && dataWriteCommand.loadType() != VisitableCommand.LoadType.DONT_LOAD
                     && ((MVCCEntry) cacheEntry).isRead()) {
                  addVersionRead((TxInvocationContext) rCtx, cacheEntry, dataWriteCommand.getKey());
               }
               ((MVCCEntry) cacheEntry).updatePreviousValue();
            }
         }
      });
   }

   // This visitor replays the entry wrapping during remote prepare.
   // The command is passed down the stack even if its keys do not belong to this node according
   // to writeCH, it's a role of TxDistributionInterceptor to ignore such command.
   // The entry is wrapped only if it's available for reading, otherwise it has to be wrapped
   // in TxDistributionInterceptor. When the entry is not wrapped, the value is not loaded in
   // CacheLoaderInterceptor, therefore passing the command down the stack causes only minimal overhead.
   private final class EntryWrappingVisitor extends AbstractVisitor {
      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         return handleWriteManyCommand(ctx, command);
      }

      @Override
      public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
         return handleWriteManyCommand(ctx, command);
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         return handleWriteCommand(ctx, command);
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return handleWriteCommand(ctx, command);
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         return handleWriteCommand(ctx, command);
      }

      @Override
      public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
         return handleWriteCommand(ctx, command);
      }

      @Override
      public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
         return handleWriteCommand(ctx, command);
      }

      @Override
      public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
         return handleWriteCommand(ctx, command);
      }

      @Override
      public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
         return handleWriteCommand(ctx, command);
      }

      @Override
      public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
         return handleWriteCommand(ctx, command);
      }

      @Override
      public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
         return handleWriteManyCommand(ctx, command);
      }

      @Override
      public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
         return handleWriteCommand(ctx, command);
      }

      @Override
      public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
         return handleWriteManyCommand(ctx, command);
      }

      @Override
      public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
         return handleWriteManyCommand(ctx, command);
      }

      @Override
      public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
         return handleWriteManyCommand(ctx, command);
      }

      private Object handleWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), ignoreOwnership(command) || canRead(command.getKey()), command.loadType() != VisitableCommand.LoadType.DONT_LOAD);
         return invokeNext(ctx, command);
      }

      private Object handleWriteManyCommand(InvocationContext ctx, WriteCommand command) {
         boolean ignoreOwnership = ignoreOwnership(command);
         for (Object key : command.getAffectedKeys()) {
            entryFactory.wrapEntryForWriting(ctx, key, ignoreOwnership || canRead(key), command.loadType() != VisitableCommand.LoadType.DONT_LOAD);
         }
         return invokeNext(ctx, command);
      }
   }

   private boolean commitEntryIfNeeded(final InvocationContext ctx, final FlagAffectedCommand command,
                                       final CacheEntry entry, final Flag stateTransferFlag) {
      if (entry == null) {
         return false;
      }
      final boolean l1Invalidation = command instanceof InvalidateL1Command;

      if (entry.isChanged()) {
         if (trace) log.tracef("About to commit entry %s", entry);
         commitContextEntry(entry, ctx, command, stateTransferFlag, l1Invalidation);

         return true;
      }
      return false;
   }

   /**
    * total order condition: only commits when it is remote context and the prepare has the flag 1PC set
    *
    * @param command the prepare command
    * @param ctx the invocation context
    * @return true if the modification should be committed, false otherwise
    */
   protected boolean shouldCommitDuringPrepare(PrepareCommand command, TxInvocationContext ctx) {
      return totalOrder ?
             command.isOnePhaseCommit() && (!ctx.isOriginLocal() || !command.hasModifications()) :
             command.isOnePhaseCommit();
   }

   protected final Object wrapEntriesForPrepareAndApply(TxInvocationContext ctx, PrepareCommand command, InvocationSuccessFunction handler) throws Throwable {
      if (!ctx.isOriginLocal() || command.isReplayEntryWrapping()) {
         return applyModificationsAndThen(ctx, command, command.getModifications(), 0, handler);
      }
      return handler.apply(ctx, command, null);
   }

   private Object applyModificationsAndThen(TxInvocationContext ctx, PrepareCommand command, WriteCommand[] modifications, int startIndex, InvocationSuccessFunction handler) throws Throwable {
      // We need to execute modifications for the same key sequentially. In theory we could optimize
      // this loop if there are multiple remote invocations but since remote invocations are rare, we omit this.
      for (int i = startIndex; i < modifications.length; i++) {
         WriteCommand c = modifications[i];
         c.setTopologyId(command.getTopologyId());
         if (c.hasAnyFlag(FlagBitSets.PUT_FOR_X_SITE_STATE_TRANSFER)) {
            ctx.getCacheTransaction().setStateTransferFlag(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
         }
         Object result = c.acceptVisitor(ctx, entryWrappingVisitor);

         if (!isSuccessfullyDone(result)) {
            int nextIndex = i + 1;
            if (nextIndex >= modifications.length) {
               return makeStage(result).thenApply(ctx, command, handler);
            }
            return makeStage(result).thenApply(ctx, command,
                  (rCtx, rCommand, rv) -> applyModificationsAndThen(ctx, command, modifications, nextIndex, handler));
         }
      }
      return handler.apply(ctx, command, null);
   }
}
