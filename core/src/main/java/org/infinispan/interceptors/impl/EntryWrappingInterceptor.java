package org.infinispan.interceptors.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.FunctionalCommand;
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
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.impl.EntryFactory;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.group.impl.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationFinallyFunction;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.remoting.responses.Response;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
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
   @Inject private EntryFactory entryFactory;
   @Inject private InternalDataContainer<Object, Object> dataContainer;
   @Inject protected ClusteringDependentLogic cdl;
   @Inject private VersionGenerator versionGenerator;
   @Inject protected DistributionManager distributionManager;
   @Inject private ComponentRef<StateConsumer> stateConsumer;
   @Inject private StateTransferLock stateTransferLock;
   @Inject private ComponentRef<XSiteStateConsumer> xSiteStateConsumer;
   @Inject private GroupManager groupManager;
   @Inject private CacheNotifier<Object, Object> notifier;
   @Inject private KeyPartitioner keyPartitioner;

   private final EntryWrappingVisitor entryWrappingVisitor = new EntryWrappingVisitor();
   private boolean isInvalidation;
   private boolean isSync;
   private boolean useRepeatableRead;
   private boolean isVersioned;

   private static final Log log = LogFactory.getLog(EntryWrappingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final long EVICT_FLAGS_BITSET =
         FlagBitSets.SKIP_OWNERSHIP_CHECK | FlagBitSets.CACHE_MODE_LOCAL;
   private boolean totalOrder;

   private void addVersionRead(InvocationContext rCtx, AbstractDataCommand dataCommand) {
      // The entry must be in the context
      CacheEntry cacheEntry = rCtx.lookupEntry(dataCommand.getKey());
      cacheEntry.setSkipLookup(true);
      if (isVersioned && ((MVCCEntry) cacheEntry).isRead()) {
         addVersionRead((TxInvocationContext) rCtx, cacheEntry, dataCommand.getKey());
      }
   }

   private final InvocationSuccessFunction dataReadReturnHandler = (rCtx, rCommand, rv) -> {
      AbstractDataCommand dataCommand = (AbstractDataCommand) rCommand;

      if (rCtx.isInTxScope() && useRepeatableRead) {
         // This invokes another method as this is only done with a specific configuration and we want to inline
         // the notifier below
         addVersionRead(rCtx, dataCommand);
      }

      // Entry visit notifications used to happen in the CallInterceptor
      // We do it at the end to avoid adding another try/finally block around the notifications
      if (rv != null && !(rv instanceof Response)) {
         Object value = dataCommand instanceof GetCacheEntryCommand ? ((CacheEntry) rv).getValue() : rv;
         CompletionStage<Void> stage = notifier.notifyCacheEntryVisited(dataCommand.getKey(), value, true, rCtx, dataCommand);
         // If stage is already complete, we can avoid allocating lambda below
         if (CompletionStages.isCompleteSuccessfully(stage)) {
            stage = notifier.notifyCacheEntryVisited(dataCommand.getKey(), value, false, rCtx, dataCommand);
         } else {
            // Make sure to fire the events serially
            stage = stage.thenCompose(v -> notifier.notifyCacheEntryVisited(dataCommand.getKey(), value, false, rCtx, dataCommand));
         }
         return delayedValue(stage, rv);
      }
      return rv;
   };

   private final InvocationSuccessFunction commitEntriesSuccessHandler = (rCtx, rCommand, rv) ->
         delayedValue(commitContextEntries(rCtx, null), rv);

   private final InvocationFinallyFunction commitEntriesFinallyHandler = this::commitEntriesFinally;
   private final InvocationSuccessFunction prepareHandler = this::prepareHandler;
   private final InvocationSuccessFunction applyAndFixVersion = this::applyAndFixVersion;
   private final InvocationSuccessFunction applyAndFixVersionForMany = this::applyAndFixVersionForMany;

   @Start
   public void start() {
      isInvalidation = cacheConfiguration.clustering().cacheMode().isInvalidation();
      isSync = cacheConfiguration.clustering().cacheMode().isSynchronous();
      // isolation level makes no sense without transactions
      useRepeatableRead = cacheConfiguration.transaction().transactionMode().isTransactional()
            && cacheConfiguration.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ
            || cacheConfiguration.clustering().cacheMode().isScattered();
      isVersioned = Configurations.isTxVersioned(cacheConfiguration);
      totalOrder = cacheConfiguration.transaction().transactionProtocol().isTotalOrder();
   }

   private boolean ignoreOwnership(FlagAffectedCommand command) {
      return distributionManager == null || command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_OWNERSHIP_CHECK);
   }

   protected boolean canRead(DataCommand command) {
      return distributionManager.getCacheTopology().isSegmentReadOwner(command.getSegment());
   }

   protected boolean canReadKey(Object key) {
      return distributionManager.getCacheTopology().isReadOwner(key);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return wrapEntriesForPrepareAndApply(ctx, command, prepareHandler);
   }

   private Object prepareHandler(InvocationContext ctx, VisitableCommand command, Object rv) {
      if (shouldCommitDuringPrepare((PrepareCommand) command, (TxInvocationContext) ctx)) {
         return invokeNextThenApply(ctx, command, commitEntriesSuccessHandler);
      } else {
         return invokeNext(ctx, command);
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNextAndHandle(ctx, command, commitEntriesFinallyHandler);
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
      entryFactory.wrapEntryForReading(ctx, key, command.getSegment(), ignoreOwnership(command) || canRead(command));
      return invokeNextThenApply(ctx, command, dataReadReturnHandler);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      boolean ignoreOwnership = ignoreOwnership(command);
      for (Object key : command.getKeys()) {
         entryFactory.wrapEntryForReading(ctx, key, keyPartitioner.getSegment(key), ignoreOwnership || canReadKey(key));
      }
      return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, t) -> {
         GetAllCommand getAllCommand = (GetAllCommand) rCommand;
         if (useRepeatableRead) {
            for (Object key : getAllCommand.getKeys()) {
               CacheEntry cacheEntry = rCtx.lookupEntry(key);
               if (cacheEntry == null) {
                  // Data was lost
                  if (trace) log.tracef(t, "Missing entry for " + key);
               } else {
                  cacheEntry.setSkipLookup(true);
               }
            }
         }

         AggregateCompletionStage<Void> stage = CompletionStages.aggregateCompletionStage();
         // Entry visit notifications used to happen in the CallInterceptor
         // instanceof check excludes the case when the command returns UnsuccessfulResponse
         if (t == null && rv instanceof Map) {
            boolean notify = !command.hasAnyFlag(FlagBitSets.SKIP_LISTENER_NOTIFICATION) && notifier.hasListener(CacheEntryVisited.class);
            log.tracef("Notifying getAll? %s; result %s", notify, rv);
            if (notify) {
               Map<Object, Object> map = (Map<Object, Object>) rv;
               for (Map.Entry<Object, Object> entry : map.entrySet()) {
                  Object value = entry.getValue();
                  if (value != null) {
                     Object finalValue = command.isReturnEntries() ? ((CacheEntry) value).getValue() : entry.getValue();
                     CompletionStage<Void> innerStage = notifier.notifyCacheEntryVisited(entry.getKey(), finalValue, true, rCtx, getAllCommand);
                     stage.dependsOn(innerStage.thenCompose(ig -> notifier.notifyCacheEntryVisited(entry.getKey(), finalValue, false, rCtx, getAllCommand)));
                  }
               }
            }
         }
         return delayedValue(stage.freeze(), rv, t);
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
            entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key), true, false);
         }
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   @Override
   public final Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         // If we are committing a ClearCommand now then no keys should be written by state transfer from
         // now on until current rebalance ends.
         if (stateConsumer.running() != null) {
            stateConsumer.running().stopApplyingState(((ClearCommand) rCommand).getTopologyId());
         }
         if (xSiteStateConsumer.running() != null) {
            xSiteStateConsumer.running().endStateTransfer(null);
         }

         CompletionStage<Void> stage = null;
         if (!rCtx.isInTxScope()) {
            ClearCommand clearCommand = (ClearCommand) rCommand;
            stage = applyChanges(rCtx, clearCommand);
         }

         if (trace)
            log.tracef("The return value is %s", rv);
         return delayedValue(stage, rv);
      });
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command)
         throws Throwable {
      for (Object key : command.getKeys()) {
         // TODO: move to distribution interceptors?
         // we need to try to wrap the entry to get it removed
         // for the removal itself, wrapping null would suffice, but listeners need previous value
         entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key), false, false);
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
      entryFactory.wrapEntryForWriting(ctx, command.getKey(), command.getSegment(), ignoreOwnership(command) || canRead(command), command.loadType() != VisitableCommand.LoadType.DONT_LOAD);
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
            log.tracef("This is a retry - resetting previous value in entry %s", entry);
         }
         if (entry != null) {
            entry.resetCurrentValue();
         }
      } else {
         if (trace) {
            log.tracef("This is a retry - removing looked up entry %s", ctx.lookupEntry(key));
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
            // When a non-transactional command is retried remotely, the context is going to be empty
            if (entry != null) {
               entry.resetCurrentValue();
            }
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
   public Object visitRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand command) throws Throwable {
      entryFactory.wrapEntryForExpired(ctx, command.getKey(), command.getSegment(),
            ignoreOwnership(command) || canRead(command));
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
         entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
               ignoreOwnership || canReadKey(key), command.loadType() != VisitableCommand.LoadType.DONT_LOAD);
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      command.setFlagsBitSet(command.getFlagsBitSet() | EVICT_FLAGS_BITSET); //to force the wrapping
      return visitRemoveCommand(ctx, command);
   }

   @Override
   public Object visitGetKeysInGroupCommand(final InvocationContext ctx, GetKeysInGroupCommand command)
         throws Throwable {
      if (command.isGroupOwner()) {
         dataContainer.forEach(internalCacheEntry -> {
            Object key = internalCacheEntry.getKey();
            if (!command.getGroupName().equals(groupManager.getGroup(key)) || ctx.lookupEntry(key) != null)
               return;

            // Don't wrap tombstones into context; we want to be able to eventually read these values from
            // cache store and the filter in CacheLoaderInterceptor ignores keys already in context
            if (internalCacheEntry.getValue() != null) {
               synchronized (ctx) {
                  //the process can be made in multiple threads, so we need to synchronize in the context.
                  entryFactory.wrapExternalEntry(ctx, key, internalCacheEntry, true, false);
               }
            }
         });
      }
      // We don't make sure that all read entries have skipLookup here, since EntryFactory does that
      // for those we have really read, and there shouldn't be any null-read entries.
      if (ctx.isInTxScope() && useRepeatableRead) {
         return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
            TxInvocationContext txCtx = (TxInvocationContext) rCtx;
            ctx.forEachEntry((key, entry) -> {
               entry.setSkipLookup(true);
               if (isVersioned && ((MVCCEntry) entry).isRead()) {
                  addVersionRead(txCtx, entry, key);
               }
            });
         });
      } else {
         return invokeNext(ctx, command);
      }
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      if (command instanceof TxReadOnlyKeyCommand) {
         // TxReadOnlyKeyCommand may apply some mutations on the entry in context so we need to always wrap it
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), command.getSegment(), ignoreOwnership(command) || canRead(command), true);
      } else {
         entryFactory.wrapEntryForReading(ctx, command.getKey(), command.getSegment(), ignoreOwnership(command) || canRead(command));
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
            entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
                  ignoreOwnership(command) || canReadKey(key), true);
         }
      } else {
         for (Object key : command.getKeys()) {
            entryFactory.wrapEntryForReading(ctx, key, keyPartitioner.getSegment(key),
                  ignoreOwnership || canReadKey(key));
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
      for (Object key : command.getArguments().keySet()) {
         //the put map never reads the keys
         entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
               ignoreOwnership || canReadKey(key), false);
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
         entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
               ignoreOwnership || canReadKey(key), false);
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
         entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
               ignoreOwnership || canReadKey(key), true);
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
         entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
               ignoreOwnership || canReadKey(key), true);
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   protected final CompletionStage<Void> commitContextEntries(InvocationContext ctx, FlagAffectedCommand command) {
      final Flag stateTransferFlag = FlagBitSets.extractStateTransferFlag(ctx, command);

      if (ctx instanceof SingleKeyNonTxInvocationContext) {
         SingleKeyNonTxInvocationContext singleKeyCtx = (SingleKeyNonTxInvocationContext) ctx;
         return commitEntryIfNeeded(ctx, command, singleKeyCtx.getKey(),
                             singleKeyCtx.getCacheEntry(), stateTransferFlag);
      } else {
         AggregateCompletionStage<Void> aggregateCompletionStage = null;
         Map<Object, CacheEntry> entries = ctx.getLookedUpEntries();
         for (Map.Entry<Object, CacheEntry> entry : entries.entrySet()) {
            CompletionStage<Void> stage = commitEntryIfNeeded(ctx, command, entry.getKey(), entry.getValue(), stateTransferFlag);
            if (!CompletionStages.isCompleteSuccessfully(stage)) {
               if (aggregateCompletionStage == null) {
                  aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
               }
               aggregateCompletionStage.dependsOn(stage);
            }
         }
         return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
      }
   }

   protected CompletionStage<Void> commitContextEntry(CacheEntry entry, InvocationContext ctx, FlagAffectedCommand command,
                                     Flag stateTransferFlag, boolean l1Invalidation) {
      return cdl.commitEntry(entry, command, ctx, stateTransferFlag, l1Invalidation);
   }

   private CompletionStage<Void> applyChanges(InvocationContext ctx, WriteCommand command) {
      stateTransferLock.acquireSharedTopologyLock();
      try {
         // We only retry non-tx write commands
         if (!isInvalidation) {
            // Can't perform the check during preload or if the cache isn't clustered
            boolean syncRpc = isSync && !command.hasAnyFlag(FlagBitSets.FORCE_ASYNCHRONOUS) ||
                  command.hasAnyFlag(FlagBitSets.FORCE_SYNCHRONOUS);
            if (command.isSuccessful() && distributionManager != null) {
               int commandTopologyId = command.getTopologyId();
               int currentTopologyId = distributionManager.getCacheTopology().getTopologyId();
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
                     throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
                  }
               }
            }
         }

         return commitContextEntries(ctx, command);
      } finally {
         stateTransferLock.releaseSharedTopologyLock();
      }
   }

   /**
    * Locks the value for the keys accessed by the command to avoid being override from a remote get.
    */
   protected Object setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(InvocationContext ctx, WriteCommand command) {
      return invokeNextThenApply(ctx, command, applyAndFixVersionForMany);
   }

   private Object applyAndFixVersionForMany(InvocationContext ctx, VisitableCommand cmd, Object rv) {
      WriteCommand writeCommand = (WriteCommand) cmd;
      if (!ctx.isInTxScope()) {
         return delayedValue(applyChanges(ctx, writeCommand), rv);
      }

      if (trace)
         log.tracef("The return value is %s", toStr(rv));
      if (useRepeatableRead) {
         boolean addVersionRead = isVersioned && writeCommand.loadType() != VisitableCommand.LoadType.DONT_LOAD;
         TxInvocationContext txCtx = (TxInvocationContext) ctx;
         for (Object key : writeCommand.getAffectedKeys()) {
            CacheEntry cacheEntry = ctx.lookupEntry(key);
            if (cacheEntry != null) {
               cacheEntry.setSkipLookup(true);
               if (addVersionRead && ((MVCCEntry) cacheEntry).isRead()) {
                  addVersionRead(txCtx, cacheEntry, key);
               }
               ((MVCCEntry) cacheEntry).updatePreviousValue();
            }
         }
      }
      return rv;
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
      return invokeNextThenApply(ctx, command, applyAndFixVersion);
   }

   private Object applyAndFixVersion(InvocationContext ctx, VisitableCommand cmd, Object rv) {
      DataWriteCommand dataWriteCommand = (DataWriteCommand) cmd;
      if (!ctx.isInTxScope()) {
         return delayedValue(applyChanges(ctx, dataWriteCommand), rv);
      }

      if (trace)
         log.tracef("The return value is %s", rv);
      if (useRepeatableRead) {
         CacheEntry cacheEntry = ctx.lookupEntry(dataWriteCommand.getKey());
         // The entry is not in context when the command's execution type does not contain origin
         if (cacheEntry != null) {
            cacheEntry.setSkipLookup(true);
            if (isVersioned && dataWriteCommand.loadType() != VisitableCommand.LoadType.DONT_LOAD
                  && ((MVCCEntry) cacheEntry).isRead()) {
               addVersionRead((TxInvocationContext) ctx, cacheEntry, dataWriteCommand.getKey());
            }
            ((MVCCEntry) cacheEntry).updatePreviousValue();
         }
      }
      return rv;
   }

   private Object commitEntriesFinally(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable t) {
      // Do not commit if the command will be retried
      if (t instanceof OutdatedTopologyException)
         return new SimpleAsyncInvocationStage(t);

      return delayedValue(commitContextEntries(rCtx, null), rv, t);
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
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), command.getSegment(),
               ignoreOwnership(command) || canRead(command), command.loadType() != VisitableCommand.LoadType.DONT_LOAD);
         return invokeNext(ctx, command);
      }

      private Object handleWriteManyCommand(InvocationContext ctx, WriteCommand command) {
         boolean ignoreOwnership = ignoreOwnership(command);
         for (Object key : command.getAffectedKeys()) {
            entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
                  ignoreOwnership || canReadKey(key), command.loadType() != VisitableCommand.LoadType.DONT_LOAD);
         }
         return invokeNext(ctx, command);
      }
   }

   private CompletionStage<Void> commitEntryIfNeeded(final InvocationContext ctx, final FlagAffectedCommand command,
         Object key, final CacheEntry entry, final Flag stateTransferFlag) {
      if (entry == null) {
         if (trace) {
            log.tracef("Entry for key %s is null : not calling commitUpdate", toStr(key));
         }
         return CompletableFutures.completedNull();
      }
      final boolean l1Invalidation = command instanceof InvalidateL1Command;

      if (entry.isChanged()) {
         if (trace) log.tracef("About to commit entry %s", entry);
         return commitContextEntry(entry, ctx, command, stateTransferFlag, l1Invalidation);
      } else if (trace) {
         log.tracef("Entry for key %s is not changed(%s): not calling commitUpdate", toStr(key), entry);
      }
      return CompletableFutures.completedNull();
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
      } else if (ctx.isOriginLocal()) {
         // If there's a functional command invoked in previous topology, it's possible that this node was not an owner
         // but now it has become one. In that case the modification was not applied into the context and we would not
         // commit the change. To be on the safe side we'll replay the whole transaction.
         for (WriteCommand mod : command.getModifications()) {
            if (mod.getTopologyId() < command.getTopologyId() && mod instanceof FunctionalCommand) {
               log.trace("Clearing looked up entries and replaying whole transaction");
               ctx.getCacheTransaction().clearLookedUpEntries();
               return applyModificationsAndThen(ctx, command, command.getModifications(), 0, handler);
            }
         }
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
