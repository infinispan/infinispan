package org.infinispan.interceptors.impl;

import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.container.impl.EntryFactory.expirationCheckDelay;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.impl.EntryFactory;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.group.impl.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.ExceptionSyncInvocationStage;
import org.infinispan.interceptors.InvocationFinallyFunction;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.WriteSkewHelper;
import org.infinispan.util.concurrent.AggregateCompletionStage;
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
   @Inject EntryFactory entryFactory;
   @Inject InternalDataContainer<Object, Object> dataContainer;
   @Inject protected ClusteringDependentLogic cdl;
   @Inject VersionGenerator versionGenerator;
   @Inject protected DistributionManager distributionManager;
   @Inject ComponentRef<StateConsumer> stateConsumer;
   @Inject StateTransferLock stateTransferLock;
   @Inject ComponentRef<XSiteStateConsumer> xSiteStateConsumer;
   @Inject GroupManager groupManager;
   @Inject CacheNotifier<Object, Object> notifier;
   @Inject KeyPartitioner keyPartitioner;

   private final EntryWrappingVisitor entryWrappingVisitor = new EntryWrappingVisitor();
   private boolean isInvalidation;
   private boolean isSync;
   private boolean useRepeatableRead;
   private boolean isVersioned;
   private boolean isPessimistic;

   private static final Log log = LogFactory.getLog(EntryWrappingInterceptor.class);
   private static final long EVICT_FLAGS_BITSET =
         FlagBitSets.SKIP_OWNERSHIP_CHECK | FlagBitSets.CACHE_MODE_LOCAL;

   private void addVersionRead(InvocationContext rCtx, AbstractDataCommand dataCommand) {
      // The entry must be in the context
      CacheEntry cacheEntry = rCtx.lookupEntry(dataCommand.getKey());
      cacheEntry.setSkipLookup(true);
      if (isVersioned && ((MVCCEntry) cacheEntry).isRead()) {
         WriteSkewHelper.addVersionRead((TxInvocationContext) rCtx, cacheEntry, dataCommand.getKey(), versionGenerator, log);
      }
   }

   private final InvocationSuccessFunction<AbstractDataCommand> dataReadReturnHandler = (rCtx, dataCommand, rv) -> {
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
         if (CompletionStages.isCompletedSuccessfully(stage)) {
            stage = notifier.notifyCacheEntryVisited(dataCommand.getKey(), value, false, rCtx, dataCommand);
         } else {
            // Make sure to fire the events serially
            stage = stage.thenCompose(v -> notifier.notifyCacheEntryVisited(dataCommand.getKey(), value, false, rCtx, dataCommand));
         }
         return delayedValue(stage, rv);
      }
      return rv;
   };

   private final InvocationSuccessFunction<VisitableCommand> commitEntriesSuccessHandler = (rCtx, rCommand, rv) ->
         delayedValue(commitContextEntries(rCtx, null), rv);

   private final InvocationFinallyFunction<CommitCommand> commitEntriesFinallyHandler = this::commitEntriesFinally;
   private final InvocationSuccessFunction<PrepareCommand> prepareHandler = this::prepareHandler;
   private final InvocationSuccessFunction<DataWriteCommand> applyAndFixVersion = this::applyAndFixVersion;
   private final InvocationSuccessFunction<WriteCommand> applyAndFixVersionForMany = this::applyAndFixVersionForMany;
   private final InvocationFinallyFunction<GetAllCommand> getAllHandleFunction = this::getAllHandle;

   @Start
   public void start() {
      isInvalidation = cacheConfiguration.clustering().cacheMode().isInvalidation();
      isSync = cacheConfiguration.clustering().cacheMode().isSynchronous();
      // isolation level makes no sense without transactions
      useRepeatableRead = cacheConfiguration.transaction().transactionMode().isTransactional()
            && cacheConfiguration.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ;
      isVersioned = Configurations.isTxVersioned(cacheConfiguration);
      isPessimistic = cacheConfiguration.transaction().transactionMode().isTransactional()
            && cacheConfiguration.transaction().lockingMode() == LockingMode.PESSIMISTIC;
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

   private Object prepareHandler(InvocationContext ctx, PrepareCommand command, Object rv) {
      if (command.isOnePhaseCommit()) {
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
      CompletionStage<Void> stage = entryFactory.wrapEntryForReading(ctx, key, command.getSegment(),
            ignoreOwnership(command) || canRead(command), command.hasAnyFlag(FlagBitSets.ALREADY_HAS_LOCK)
                  || (isPessimistic && command.hasAnyFlag(FlagBitSets.FORCE_WRITE_LOCK)), CompletableFutures.completedNull());
      return makeStage(asyncInvokeNext(ctx, command, stage)).thenApply(ctx, command, dataReadReturnHandler);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      boolean ignoreOwnership = ignoreOwnership(command);
      CompletableFuture<Void> initialStage = new CompletableFuture<>();
      CompletionStage<Void> currentStage = initialStage;
      for (Object key : command.getKeys()) {
         currentStage = entryFactory.wrapEntryForReading(ctx, key, keyPartitioner.getSegment(key),
                                                         ignoreOwnership || canReadKey(key), false,
                                                         currentStage);
      }

      return makeStage(asyncInvokeNext(ctx, command, expirationCheckDelay(currentStage, initialStage)))
            .andHandle(ctx, command, getAllHandleFunction);
   }

   private Object getAllHandle(InvocationContext rCtx, GetAllCommand command, Object rv, Throwable t) {
      if (useRepeatableRead) {
         for (Object key : command.getKeys()) {
            CacheEntry cacheEntry = rCtx.lookupEntry(key);
            if (cacheEntry == null) {
               // Data was lost
               if (log.isTraceEnabled()) log.tracef(t, "Missing entry for " + key);
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
                  CompletionStage<Void> innerStage = notifier.notifyCacheEntryVisited(entry.getKey(), finalValue, true, rCtx, command);
                  stage.dependsOn(innerStage.thenCompose(ig -> notifier.notifyCacheEntryVisited(entry.getKey(), finalValue, false, rCtx, command)));
               }
            }
         }
      }
      return delayedValue(stage.freeze(), rv, t);
   }

   @Override
   public final Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) {
      CompletableFuture<Void> initialStage = new CompletableFuture<>();
      CompletionStage<Void> currentStage = initialStage;
      if (command.getKeys() != null) {
         for (Object key : command.getKeys()) {
            // TODO: move this to distribution interceptors?
            // we need to try to wrap the entry to get it removed
            // for the removal itself, wrapping null would suffice, but listeners need previous value
            currentStage = entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
                                                                           true, false, currentStage);
         }
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command, expirationCheckDelay(currentStage, initialStage));
   }

   @Override
   public final Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         // If we are committing a ClearCommand now then no keys should be written by state transfer from
         // now on until current rebalance ends.
         if (stateConsumer.running() != null) {
            stateConsumer.running().stopApplyingState(rCommand.getTopologyId());
         }
         if (xSiteStateConsumer.running() != null) {
            xSiteStateConsumer.running().endStateTransfer(null);
         }

         CompletionStage<Void> stage = null;
         if (!rCtx.isInTxScope()) {
            stage = applyChanges(rCtx, rCommand);
         }

         if (log.isTraceEnabled())
            log.tracef("The return value is %s", rv);
         return delayedValue(stage, rv);
      });
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) {
      CompletableFuture<Void> initialStage = new CompletableFuture<>();
      CompletionStage<Void> currentStage = initialStage;
      for (Object key : command.getKeys()) {
         // TODO: move to distribution interceptors?
         // we need to try to wrap the entry to get it removed
         // for the removal itself, wrapping null would suffice, but listeners need previous value
         currentStage = entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
                                                                        false, false, currentStage);
         if (log.isTraceEnabled())
           log.tracef("Entry to be removed: %s", toStr(key));
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command, expirationCheckDelay(currentStage, initialStage));
   }

   @Override
   public final Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, wrapEntryIfNeeded(ctx, command));
   }

   @Override
   public final Object visitIracPutKeyValueCommand(InvocationContext ctx, IracPutKeyValueCommand command) {
      boolean isOwner = ignoreOwnership(command) || canRead(command);
      entryFactory.wrapEntryForWritingSkipExpiration(ctx, command.getKey(), command.getSegment(), isOwner);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, CompletableFutures.completedNull());
   }

   protected CompletionStage<Void> wrapEntryIfNeeded(InvocationContext ctx, AbstractDataWriteCommand command) {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getKey());
      }

      // Non-owners should not wrap entries (unless L1 is enabled)
      boolean isOwner = ignoreOwnership(command) || canRead(command);

      if (command.hasAnyFlag(FlagBitSets.BACKUP_WRITE)) {
         // The command has been forwarded to a backup, we don't care if the entry is expired or not
         entryFactory.wrapEntryForWritingSkipExpiration(ctx, command.getKey(), command.getSegment(), isOwner);
         return CompletableFutures.completedNull();
      }
      return entryFactory.wrapEntryForWriting(ctx, command.getKey(), command.getSegment(), isOwner,
                                              command.loadType() != VisitableCommand.LoadType.DONT_LOAD,
                                              CompletableFutures.completedNull());
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
         if (log.isTraceEnabled()) {
            log.tracef("This is a retry - resetting previous value in entry %s", entry);
         }
         if (entry != null) {
            entry.resetCurrentValue();
         }
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("This is a retry - removing looked up entry %s", ctx.lookupEntry(key));
         }
         ctx.removeLookedUpEntry(key);
      }
   }

   private void removeFromContextOnRetry(InvocationContext ctx, Collection<?> keys) {
      if (useRepeatableRead) {
         if (log.isTraceEnabled()) {
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
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, wrapEntryIfNeeded(ctx, command));
   }

   @Override
   public Object visitRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand command) throws Throwable {
      boolean isOwner = ignoreOwnership(command) || canRead(command);
      entryFactory.wrapEntryForWritingSkipExpiration(ctx, command.getKey(), command.getSegment(), isOwner);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, CompletableFutures.completedNull());
   }

   @Override
   public final Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
         throws Throwable {
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, wrapEntryIfNeeded(ctx, command));
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, wrapEntryIfNeeded(ctx, command));
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, wrapEntryIfNeeded(ctx, command));
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      boolean ignoreOwnership = ignoreOwnership(command);
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      CompletableFuture<Void> initialStage = new CompletableFuture<>();
      CompletionStage<Void> currentStage = initialStage;
      for (Object key : command.getMap().keySet()) {
         // as listeners may need the value, we'll load the previous value
         currentStage = entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
               ignoreOwnership || canReadKey(key), command.loadType() != VisitableCommand.LoadType.DONT_LOAD, currentStage);
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command, expirationCheckDelay(currentStage, initialStage));
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      command.setFlagsBitSet(command.getFlagsBitSet() | EVICT_FLAGS_BITSET); //to force the wrapping
      return visitRemoveCommand(ctx, command);
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      CompletionStage<Void> stage;
      if (command instanceof TxReadOnlyKeyCommand) {
         // TxReadOnlyKeyCommand may apply some mutations on the entry in context so we need to always wrap it
         stage = entryFactory.wrapEntryForWriting(ctx, command.getKey(), command.getSegment(),
                                                  ignoreOwnership(command) || canRead(command), true,
                                                  CompletableFutures.completedNull());
      } else {
         stage = entryFactory.wrapEntryForReading(ctx, command.getKey(), command.getSegment(),
                                                  ignoreOwnership(command) || canRead(command), false,
                                                  CompletableFutures.completedNull());
      }

      // Repeatable reads are not achievable with functional commands, as we don't store the value locally
      // and we don't "fix" it on the remote node; therefore, the value will be able to change and identity read
      // could return different values in the same transaction.
      // (Note: at this point TX mode is not implemented for functional commands anyway).
      return asyncInvokeNext(ctx, command, stage);
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      boolean ignoreOwnership = ignoreOwnership(command);
      CompletableFuture<Void> initialStage = new CompletableFuture<>();
      CompletionStage<Void> currentStage = initialStage;
      if (command instanceof TxReadOnlyManyCommand) {
         // TxReadOnlyManyCommand may apply some mutations on the entry in context so we need to always wrap it
         for (Object key : command.getKeys()) {
            // TODO: need to handle this
            currentStage = entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
                  ignoreOwnership(command) || canReadKey(key), true, currentStage);
         }
      } else {
         for (Object key : command.getKeys()) {
            currentStage = entryFactory.wrapEntryForReading(ctx, key, keyPartitioner.getSegment(key),
                  ignoreOwnership || canReadKey(key), false, currentStage);
         }
      }
      // Repeatable reads are not achievable with functional commands, see visitReadOnlyKeyCommand
      return asyncInvokeNext(ctx, command, expirationCheckDelay(currentStage, initialStage));
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command)
         throws Throwable {
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, wrapEntryIfNeeded(ctx, command));
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command)
         throws Throwable {
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, wrapEntryIfNeeded(ctx, command));
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable {
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, wrapEntryIfNeeded(ctx, command));
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx,
                                                  WriteOnlyManyEntriesCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      boolean ignoreOwnership = ignoreOwnership(command);
      CompletableFuture<Void> initialStage = new CompletableFuture<>();
      CompletionStage<Void> currentStage = initialStage;
      for (Object key : command.getArguments().keySet()) {
         //the put map never reads the keys
         currentStage = entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
               ignoreOwnership || canReadKey(key), false, currentStage);
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command, expirationCheckDelay(currentStage, initialStage));
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command)
         throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      boolean ignoreOwnership = ignoreOwnership(command);
      CompletableFuture<Void> initialStage = new CompletableFuture<>();
      CompletionStage<Void> currentStage = initialStage;
      for (Object key : command.getAffectedKeys()) {
         currentStage = entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
               ignoreOwnership || canReadKey(key), false, currentStage);
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command, expirationCheckDelay(currentStage, initialStage));
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command)
         throws Throwable {
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, wrapEntryIfNeeded(ctx, command));
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command)
         throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      boolean ignoreOwnership = ignoreOwnership(command);
      CompletableFuture<Void> initialStage = new CompletableFuture<>();
      CompletionStage<Void> currentStage = initialStage;
      for (Object key : command.getAffectedKeys()) {
         currentStage = entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
               ignoreOwnership || canReadKey(key), true, currentStage);
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command, expirationCheckDelay(currentStage, initialStage));
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx,
                                                  ReadWriteManyEntriesCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      boolean ignoreOwnership = ignoreOwnership(command);
      CompletableFuture<Void> initialStage = new CompletableFuture<>();
      CompletionStage<Void> currentStage = initialStage;
      for (Object key : command.getAffectedKeys()) {
         currentStage = entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
               ignoreOwnership || canReadKey(key), true, currentStage);
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command, expirationCheckDelay(currentStage, initialStage));
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
            if (!CompletionStages.isCompletedSuccessfully(stage)) {
               if (aggregateCompletionStage == null) {
                  aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
               }
               aggregateCompletionStage.dependsOn(stage);
            }
         }
         return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
      }
   }

   protected CompletionStage<Void> commitContextEntry(CacheEntry<?, ?> entry, InvocationContext ctx, FlagAffectedCommand command,
                                     Flag stateTransferFlag, boolean l1Invalidation) {
      return cdl.commitEntry(entry, command, ctx, stateTransferFlag, l1Invalidation);
   }

   private boolean needTopologyCheck(WriteCommand command) {
      // No retry in local or invalidation caches
      if (isInvalidation || distributionManager == null || command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL))
         return false;

      // Async commands cannot be retried
      return isSync && !command.hasAnyFlag(FlagBitSets.FORCE_ASYNCHRONOUS) ||
             command.hasAnyFlag(FlagBitSets.FORCE_SYNCHRONOUS);
   }

   private LocalizedCacheTopology checkTopology(WriteCommand command,
                                                LocalizedCacheTopology commandTopology) {
      int commandTopologyId = command.getTopologyId();
      LocalizedCacheTopology currentTopology = distributionManager.getCacheTopology();
      int currentTopologyId = currentTopology.getTopologyId();
      // Can't perform the check during preload or if the cache isn't clustered
      if (currentTopologyId == commandTopologyId || commandTopologyId == -1)
         return currentTopology;

      if (commandTopology != null) {
         // commandTopology != null means the modification is already committed to the data container
         // We want to retry the command if new owners were added since the command's topology,
         // but if there are no new owners retrying won't help.
         IntSet segments;
         boolean ownersAdded;
         if (command instanceof DataCommand) {
            int segment = ((DataCommand) command).getSegment();
            ownersAdded = segmentAddedOwners(commandTopology, currentTopology, segment);
         } else {
            segments = IntSets.mutableEmptySet(currentTopology.getNumSegments());
            for (Object key : command.getAffectedKeys()) {
               int segment = keyPartitioner.getSegment(key);
               segments.set(segment);
            }
            ownersAdded = false;
            for (int segment : segments) {
               if (segmentAddedOwners(commandTopology, currentTopology, segment)) {
                  ownersAdded = true;
                  break;
               }
            }
         }
         if (!ownersAdded) {
            if (log.isTraceEnabled())
               log.tracef("Cache topology changed but no owners were added for keys %s", command.getAffectedKeys());
            return null;
         }
      }

      if (log.isTraceEnabled()) log.tracef("Cache topology changed while the command was executing: expected %d, got %d",
                                           commandTopologyId, currentTopologyId);
      // This shouldn't be necessary, as we'll have a fresh command instance when retrying
      command.setValueMatcher(command.getValueMatcher().matcherForRetry());
      throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
   }

   private boolean segmentAddedOwners(LocalizedCacheTopology commandTopology, LocalizedCacheTopology currentTopology,
                                      int segment) {
      List<Address> commandTopologyOwners = commandTopology.getSegmentDistribution(segment).writeOwners();
      List<Address> currentOwners = currentTopology.getDistribution(segment).writeOwners();
      return !commandTopologyOwners.containsAll(currentOwners);
   }

   private CompletionStage<Void> applyChanges(InvocationContext ctx, WriteCommand command) {
      // Unsuccessful commands do not modify anything so they don't need to be retried either
      if (!command.isSuccessful())
         return CompletableFutures.completedNull();

      boolean needTopologyCheck = needTopologyCheck(command);
      LocalizedCacheTopology commandTopology = needTopologyCheck ? checkTopology(command, null) : null;

      CompletionStage<Void> cs = commitContextEntries(ctx, command);
      if (needTopologyCheck) {
         if (CompletionStages.isCompletedSuccessfully(cs)) {
            checkTopology(command, commandTopology);
         } else {
            return cs.thenRun(() -> checkTopology(command, commandTopology));
         }
      }
      return cs;
   }

   /**
    * Locks the value for the keys accessed by the command to avoid being override from a remote get.
    */
   protected Object setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(InvocationContext ctx, WriteCommand command,
         CompletionStage<Void> delay) {
      return makeStage(asyncInvokeNext(ctx, command, delay)).thenApply(ctx, command, applyAndFixVersionForMany);
   }

   private Object applyAndFixVersionForMany(InvocationContext ctx, WriteCommand writeCommand, Object rv) {
      if (!ctx.isInTxScope()) {
         return delayedValue(applyChanges(ctx, writeCommand), rv);
      }

      if (log.isTraceEnabled())
         log.tracef("The return value is %s", toStr(rv));
      if (useRepeatableRead) {
         boolean addVersionRead = isVersioned && writeCommand.loadType() != VisitableCommand.LoadType.DONT_LOAD;
         TxInvocationContext txCtx = (TxInvocationContext) ctx;
         for (Object key : writeCommand.getAffectedKeys()) {
            CacheEntry cacheEntry = ctx.lookupEntry(key);
            if (cacheEntry != null) {
               cacheEntry.setSkipLookup(true);
               if (addVersionRead && ((MVCCEntry) cacheEntry).isRead()) {
                  WriteSkewHelper.addVersionRead(txCtx, cacheEntry, key, versionGenerator, log);
               }
            }
         }
      }
      return rv;
   }

   /**
    * Locks the value for the keys accessed by the command to avoid being override from a remote get.
    */
   protected Object setSkipRemoteGetsAndInvokeNextForDataCommand(InvocationContext ctx,
                                                               DataWriteCommand command, CompletionStage<Void> delay) {
      return makeStage(asyncInvokeNext(ctx, command, delay)).thenApply(ctx, command, applyAndFixVersion);
   }

   private Object applyAndFixVersion(InvocationContext ctx, DataWriteCommand dataWriteCommand, Object rv) {
      if (!ctx.isInTxScope()) {
         return delayedValue(applyChanges(ctx, dataWriteCommand), rv);
      }

      if (log.isTraceEnabled())
         log.tracef("The return value is %s", rv);
      if (useRepeatableRead) {
         CacheEntry cacheEntry = ctx.lookupEntry(dataWriteCommand.getKey());
         // The entry is not in context when the command's execution type does not contain origin
         if (cacheEntry != null) {
            cacheEntry.setSkipLookup(true);
            if (isVersioned && dataWriteCommand.loadType() != VisitableCommand.LoadType.DONT_LOAD
                  && ((MVCCEntry) cacheEntry).isRead()) {
               WriteSkewHelper.addVersionRead((TxInvocationContext) ctx, cacheEntry, dataWriteCommand.getKey(), versionGenerator, log);
            }
         }
      }
      return rv;
   }

   private Object commitEntriesFinally(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable t) {
      // Do not commit if the command will be retried
      if (t instanceof OutdatedTopologyException)
         return new ExceptionSyncInvocationStage(t);

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

      @Override
      public Object visitRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand command) throws Throwable {
         boolean isOwner = ignoreOwnership(command) || canRead(command);
         entryFactory.wrapEntryForWritingSkipExpiration(ctx, command.getKey(), command.getSegment(), isOwner);
         return invokeNext(ctx, command);
      }

      private Object handleWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
         CompletionStage<Void> stage = entryFactory.wrapEntryForWriting(ctx, command.getKey(), command.getSegment(),
               ignoreOwnership(command) || canRead(command), command.loadType() != VisitableCommand.LoadType.DONT_LOAD,
                                                                        CompletableFutures.completedNull());
         return asyncInvokeNext(ctx, command, stage);
      }

      private Object handleWriteManyCommand(InvocationContext ctx, WriteCommand command) {
         boolean ignoreOwnership = ignoreOwnership(command);
         CompletableFuture<Void> initialStage = new CompletableFuture<>();
         CompletionStage<Void> currentStage = initialStage;
         for (Object key : command.getAffectedKeys()) {
            currentStage = entryFactory.wrapEntryForWriting(ctx, key, keyPartitioner.getSegment(key),
                  ignoreOwnership || canReadKey(key), command.loadType() != VisitableCommand.LoadType.DONT_LOAD, currentStage);
         }
         return asyncInvokeNext(ctx, command, expirationCheckDelay(currentStage, initialStage));
      }
   }

   private CompletionStage<Void> commitEntryIfNeeded(final InvocationContext ctx, final FlagAffectedCommand command,
         Object key, final CacheEntry entry, final Flag stateTransferFlag) {
      if (entry == null) {
         if (log.isTraceEnabled()) {
            log.tracef("Entry for key %s is null : not calling commitUpdate", toStr(key));
         }
         return CompletableFutures.completedNull();
      }
      final boolean l1Invalidation = command instanceof InvalidateL1Command;

      if (entry.isChanged()) {
         if (log.isTraceEnabled()) log.tracef("About to commit entry %s", entry);
         return commitContextEntry(entry, ctx, command, stateTransferFlag, l1Invalidation);
      } else if (log.isTraceEnabled()) {
         log.tracef("Entry for key %s is not changed(%s): not calling commitUpdate", toStr(key), entry);
      }
      return CompletableFutures.completedNull();
   }

   protected final <P extends PrepareCommand> Object wrapEntriesForPrepareAndApply(TxInvocationContext ctx, P command, InvocationSuccessFunction<P> handler) throws Throwable {
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

   private <P extends PrepareCommand> Object applyModificationsAndThen(TxInvocationContext ctx, P command, List<WriteCommand> modifications, int startIndex, InvocationSuccessFunction<P> handler) throws Throwable {
      // We need to execute modifications for the same key sequentially. In theory we could optimize
      // this loop if there are multiple remote invocations but since remote invocations are rare, we omit this.
      for (int i = startIndex; i < modifications.size(); i++) {
         WriteCommand c = modifications.get(i);
         c.setTopologyId(command.getTopologyId());
         if (c.hasAnyFlag(FlagBitSets.PUT_FOR_X_SITE_STATE_TRANSFER)) {
            ctx.getCacheTransaction().setStateTransferFlag(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
         }
         Object result = c.acceptVisitor(ctx, entryWrappingVisitor);

         if (!isSuccessfullyDone(result)) {
            int nextIndex = i + 1;
            if (nextIndex >= modifications.size()) {
               return makeStage(result).thenApply(ctx, command, handler);
            }
            return makeStage(result).thenApply(ctx, command,
                  (rCtx, rCommand, rv) -> applyModificationsAndThen(ctx, command, modifications, nextIndex, handler));
         }
      }
      return handler.apply(ctx, command, null);
   }
}
