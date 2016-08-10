package org.infinispan.interceptors.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
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
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.group.GroupFilter;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.CompositeKeyFilter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationFinallyHandler;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessHandler;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
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
   private final EntryWrappingVisitor entryWrappingVisitor = new EntryWrappingVisitor();
   private boolean isInvalidation;
   private boolean isSync;
   private StateConsumer stateConsumer;       // optional
   private StateTransferLock stateTransferLock;
   private XSiteStateConsumer xSiteStateConsumer;
   private GroupManager groupManager;
   private CacheNotifier notifier;
   private StateTransferManager stateTransferManager;
   private Address localAddress;
   private boolean useRepeatableRead;
   private boolean writeSkewCheck;

   private static final Log log = LogFactory.getLog(EntryWrappingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final EnumSet<Flag> EVICT_FLAGS =
         EnumSet.of(Flag.SKIP_OWNERSHIP_CHECK, Flag.CACHE_MODE_LOCAL);

   private final InvocationSuccessHandler dataReadReturnHandler = (rCtx, rCommand, rv) -> {
      AbstractDataCommand dataCommand = (AbstractDataCommand) rCommand;

      // TODO needed because entries might be added in L1?
      if (!rCtx.isInTxScope()) {
         commitContextEntries(rCtx, dataCommand, null);
      } else if (useRepeatableRead) {
         // The entry must be in the context
         CacheEntry cacheEntry = rCtx.lookupEntry(dataCommand.getKey());
         cacheEntry.setSkipLookup(true);
         if (writeSkewCheck) {
            addVersionRead((TxInvocationContext) rCtx, cacheEntry, dataCommand.getKey());
         }
      }

      // Entry visit notifications used to happen in the CallInterceptor
      // We do it after (maybe) committing the entries, to avoid adding another try/finally block
      if (rv != null && !(rv instanceof Response)) {
         Object value = dataCommand instanceof GetCacheEntryCommand ? ((CacheEntry) rv).getValue() : rv;
         notifier.notifyCacheEntryVisited(dataCommand.getKey(), value, true, rCtx, dataCommand);
         notifier.notifyCacheEntryVisited(dataCommand.getKey(), value, false, rCtx, dataCommand);
      }
   };

   private final InvocationSuccessHandler commitEntriesSuccessHandler = (rCtx, rCommand, rv) -> commitContextEntries(rCtx, null, null);

   private final InvocationFinallyHandler commitEntriesFinallyHandler = (rCtx, rCommand, rv, t) -> commitContextEntries(rCtx, null, null);

   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(EntryFactory entryFactory, DataContainer<Object, Object> dataContainer, ClusteringDependentLogic cdl,
                    StateConsumer stateConsumer, StateTransferLock stateTransferLock,
                    XSiteStateConsumer xSiteStateConsumer, GroupManager groupManager, CacheNotifier notifier,
                    StateTransferManager stateTransferManager, VersionGenerator versionGenerator) {
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
   }

   @Start
   public void start() {
      isInvalidation = cacheConfiguration.clustering().cacheMode().isInvalidation();
      isSync = cacheConfiguration.clustering().cacheMode().isSynchronous();
      localAddress = cdl.getAddress();
      // isolation level makes no sense without transactions
      useRepeatableRead = cacheConfiguration.transaction().transactionMode().isTransactional()
            && cacheConfiguration.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ;
      writeSkewCheck = cacheConfiguration.locking().writeSkewCheck();
   }

   private boolean ignoreOwnership(FlagAffectedCommand command) {
      return command.hasFlag(Flag.CACHE_MODE_LOCAL) || command.hasFlag(Flag.SKIP_OWNERSHIP_CHECK);
   }

   private boolean canRead(Object key) {
      // STM is null in local cache
      return stateTransferManager == null || stateTransferManager.getCacheTopology().getReadConsistentHash().isKeyLocalToNode(localAddress, key);
   }

   @Override
   public BasicInvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      wrapEntriesForPrepare(ctx, command);
      if (!shouldCommitDuringPrepare(command, ctx)) {
         return invokeNext(ctx, command);
      }
      return invokeNext(ctx, command).thenAccept(commitEntriesSuccessHandler);
   }

   @Override
   public BasicInvocationStage visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNext(ctx, command).handle(commitEntriesFinallyHandler);
   }

   @Override
   public final BasicInvocationStage visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command)
         throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   @Override
   public final BasicInvocationStage visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   private BasicInvocationStage visitDataReadCommand(InvocationContext ctx, AbstractDataCommand command) {
      entryFactory.wrapEntryForReading(ctx, command.getKey(), ignoreOwnership(command) || canRead(command.getKey()));
      return invokeNext(ctx, command).thenAccept(dataReadReturnHandler);
   }

   @Override
   public BasicInvocationStage visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      boolean ignoreOwnership = ignoreOwnership(command);
      for (Object key : command.getKeys()) {
         entryFactory.wrapEntryForReading(ctx, key, ignoreOwnership || canRead(key));
      }
      return invokeNext(ctx, command).handle((rCtx, rCommand, rv, t) -> {
         GetAllCommand getAllCommand = (GetAllCommand) rCommand;
         if (useRepeatableRead) {
            for (Object key : getAllCommand.getKeys()) {
               rCtx.lookupEntry(key).setSkipLookup(true);
            }
         }

         // Entry visit notifications used to happen in the CallInterceptor
         // instanceof check excludes the case when the command returns UnsuccessfulResponse
         if (t == null && rv instanceof Map) {
            log.tracef("Notifying getAll? %s; result %s", !command.hasFlag(Flag.SKIP_LISTENER_NOTIFICATION), rv);
            Map<Object, Object> map = (Map<Object, Object>) rv;
            // TODO: it would be nice to know if a listener was registered for this and
            // not do the full iteration if there was no visitor listener registered
            if (!command.hasFlag(Flag.SKIP_LISTENER_NOTIFICATION)) {
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
   public final BasicInvocationStage visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command)
         throws Throwable {
      if (command.getKeys() != null) {
         for (Object key : command.getKeys()) {
            // TODO: move this to distribution interceptors?
            // we need to try to wrap the entry to get it removed
            // for the removal itself, wrapping null would suffice, but listeners need previous value
            entryFactory.wrapEntryForWriting(ctx, key, true);
         }
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   @Override
   public final BasicInvocationStage visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNext(ctx, command).thenAccept((rCtx, rCommand, rv) -> {
         // If we are committing a ClearCommand now then no keys should be written by state transfer from
         // now on until current rebalance ends.
         if (stateConsumer != null) {
            stateConsumer.stopApplyingState();
         }
         if (xSiteStateConsumer != null) {
            xSiteStateConsumer.endStateTransfer(null);
         }

         if (!rCtx.isInTxScope()) {
            ClearCommand clearCommand = (ClearCommand) rCommand;
            applyChanges(rCtx, clearCommand, null);
         }

         if (trace)
            log.tracef("The return value is %s", rv);
      });
   }

   @Override
   public BasicInvocationStage visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command)
         throws Throwable {
      for (Object key : command.getKeys()) {
         // TODO: move to distribution interceptors?
         // we need to try to wrap the entry to get it removed
         // for the removal itself, wrapping null would suffice, but listeners need previous value
         entryFactory.wrapEntryForWriting(ctx, key, false);
         if (trace)
           log.tracef("Entry to be removed: %s", toStr(key));
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   @Override
   public final BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, command.getMetadata());
   }

   private void wrapEntryIfNeeded(InvocationContext ctx, AbstractDataWriteCommand command) throws Throwable {
      if (command.hasFlag(Flag.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getKey());
      }
      entryFactory.wrapEntryForWriting(ctx, command.getKey(), ignoreOwnership(command) || canRead(command.getKey()));
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
         for (Object key : keys) {
            MVCCEntry entry = (MVCCEntry) ctx.lookupEntry(key);
            if (trace) {
               log.tracef("This is a retry - resetting previous value in entry ", entry);
            }
            entry.resetCurrentValue();
         }
      } else {
         for (Object key : keys) {
            if (trace) {
               log.tracef("This is a retry - removing looked up entry " + ctx.lookupEntry(key));
            }
            ctx.removeLookedUpEntry(key);
         }
      }
   }

   @Override
   public BasicInvocationStage visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      if (command.hasFlag(Flag.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getKey());
      }
      entryFactory.wrapEntryForDelta(ctx, command.getKey(), command.getDelta(), ignoreOwnership(command) || canRead(command.getKey()));
      return invokeNext(ctx, command);
   }

   @Override
   public final BasicInvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, null);
   }

   @Override
   public final BasicInvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
         throws Throwable {
      if (command.hasFlag(Flag.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getKey());
      }
      // When retrying, we might still need to perform the command even if the previous value was removed
      entryFactory.wrapEntryForWriting(ctx, command.getKey(), ignoreOwnership(command) || canRead(command.getKey()));
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, command.getMetadata());
   }

   @Override
   public BasicInvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      boolean ignoreOwnership = ignoreOwnership(command);
      if (command.hasFlag(Flag.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      for (Object key : command.getMap().keySet()) {
         // as listeners may need the value, we'll load the previous value
         entryFactory.wrapEntryForWriting(ctx, key, ignoreOwnership || canRead(key));
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      command.addFlags(EVICT_FLAGS); //to force the wrapping
      return visitRemoveCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitGetKeysInGroupCommand(final InvocationContext ctx, GetKeysInGroupCommand command)
         throws Throwable {
      final String groupName = command.getGroupName();
      if (command.isGroupOwner()) {
         final KeyFilter<Object> keyFilter = new CompositeKeyFilter<>(new GroupFilter<>(groupName, groupManager),
               new CollectionKeyFilter<>(ctx.getLookedUpEntries().keySet()));
         dataContainer.executeTask(keyFilter, (o, internalCacheEntry) -> {
            synchronized (ctx) {
               //the process can be made in multiple threads, so we need to synchronize in the context.
               entryFactory.wrapExternalEntry(ctx, internalCacheEntry.getKey(), internalCacheEntry, false);
            }
         });
      }
      // We don't make sure that all read entries have skipLookup here, since EntryFactory does that
      // for those we have really read, and there shouldn't be any null-read entries.
      InvocationStage stage = invokeNext(ctx, command);
      if (ctx.isInTxScope() && useRepeatableRead) {
         return stage.thenAccept((rCtx, rCommand, rv) -> {
            TxInvocationContext txCtx = (TxInvocationContext) rCtx;
            for (Map.Entry<Object, CacheEntry> keyEntry : txCtx.getLookedUpEntries().entrySet()) {
               CacheEntry cacheEntry = keyEntry.getValue();
               cacheEntry.setSkipLookup(true);
               if (writeSkewCheck) {
                  addVersionRead(txCtx, cacheEntry, keyEntry.getKey());
               }
            }
         });
      } else {
         return stage;
      }
   }

   @Override
   public BasicInvocationStage visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      entryFactory.wrapEntryForReading(ctx, command.getKey(), ignoreOwnership(command) || canRead(command.getKey()));

      // Repeatable reads are not achievable with functional commands, as we don't store the value locally
      // and we don't "fix" it on the remote node; therefore, the value will be able to change and identity read
      // could return different values in the same transaction.
      // (Note: at this point TX mode is not implemented for functional commands anyway).
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      boolean ignoreOwnership = ignoreOwnership(command);
      for (Object key : command.getKeys()) {
         entryFactory.wrapEntryForReading(ctx, key, ignoreOwnership || canRead(key));
      }
      // Repeatable reads are not achievable with functional commands, see visitReadOnlyKeyCommand
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command)
         throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, null);
   }

   @Override
   public BasicInvocationStage visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command)
         throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, null);
   }

   @Override
   public BasicInvocationStage visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, null);
   }

   @Override
   public BasicInvocationStage visitWriteOnlyManyEntriesCommand(InvocationContext ctx,
         WriteOnlyManyEntriesCommand command) throws Throwable {
      if (command.hasFlag(Flag.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      boolean ignoreOwnership = ignoreOwnership(command);
      for (Object key : command.getEntries().keySet()) {
         //the put map never reads the keys
         entryFactory.wrapEntryForWriting(ctx, key, ignoreOwnership || canRead(key));
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command)
         throws Throwable {
      if (command.hasFlag(Flag.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      boolean ignoreOwnership = ignoreOwnership(command);
      for (Object key : command.getAffectedKeys()) {
         entryFactory.wrapEntryForWriting(ctx, key, ignoreOwnership || canRead(key));
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command)
         throws Throwable {
      wrapEntryIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, null);
   }

   @Override
   public BasicInvocationStage visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command)
         throws Throwable {
      if (command.hasFlag(Flag.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      boolean ignoreOwnership = ignoreOwnership(command);
      for (Object key : command.getAffectedKeys()) {
         entryFactory.wrapEntryForWriting(ctx, key, ignoreOwnership || canRead(key));
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadWriteManyEntriesCommand(InvocationContext ctx,
         ReadWriteManyEntriesCommand command) throws Throwable {
      if (command.hasFlag(Flag.COMMAND_RETRY)) {
         removeFromContextOnRetry(ctx, command.getAffectedKeys());
      }
      boolean ignoreOwnership = ignoreOwnership(command);
      for (Object key : command.getAffectedKeys()) {
         entryFactory.wrapEntryForWriting(ctx, key, ignoreOwnership || canRead(key));
      }
      return setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(ctx, command);
   }

   private Flag extractStateTransferFlag(InvocationContext ctx, FlagAffectedCommand command) {
      if (command == null) {
         //commit command
         return ctx instanceof TxInvocationContext ?
               ((TxInvocationContext) ctx).getCacheTransaction().getStateTransferFlag() :
               null;
      } else {
         if (command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
            return Flag.PUT_FOR_STATE_TRANSFER;
         } else if (command.hasFlag(Flag.PUT_FOR_X_SITE_STATE_TRANSFER)) {
            return Flag.PUT_FOR_X_SITE_STATE_TRANSFER;
         }
      }
      return null;
   }

   protected final void commitContextEntries(InvocationContext ctx, FlagAffectedCommand command, Metadata metadata) {
      final Flag stateTransferFlag = extractStateTransferFlag(ctx, command);

      if (ctx instanceof SingleKeyNonTxInvocationContext) {
         SingleKeyNonTxInvocationContext singleKeyCtx = (SingleKeyNonTxInvocationContext) ctx;
         commitEntryIfNeeded(ctx, command,
                             singleKeyCtx.getCacheEntry(), stateTransferFlag, metadata);
      } else {
         Set<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
         Iterator<Map.Entry<Object, CacheEntry>> it = entries.iterator();
         final Log log = getLog();
         while (it.hasNext()) {
            Map.Entry<Object, CacheEntry> e = it.next();
            CacheEntry entry = e.getValue();
            if (!commitEntryIfNeeded(ctx, command, entry, stateTransferFlag, metadata)) {
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
                                     Metadata metadata, Flag stateTransferFlag, boolean l1Invalidation) {
      cdl.commitEntry(entry, metadata, command, ctx, stateTransferFlag, l1Invalidation);
   }

   private void applyChanges(InvocationContext ctx, WriteCommand command, Metadata metadata) {
      stateTransferLock.acquireSharedTopologyLock();
      try {
         // We only retry non-tx write commands
         if (!isInvalidation) {
            // Can't perform the check during preload or if the cache isn't clustered
            boolean syncRpc = isSync && !command.hasFlag(Flag.FORCE_ASYNCHRONOUS) ||
                  command.hasFlag(Flag.FORCE_SYNCHRONOUS);
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

         commitContextEntries(ctx, command, metadata);
      } finally {
         stateTransferLock.releaseSharedTopologyLock();
      }
   }

   /**
    * Locks the value for the keys accessed by the command to avoid being override from a remote get.
    */
   private BasicInvocationStage setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(InvocationContext ctx, WriteCommand command) {
      return invokeNext(ctx, command).thenAccept((rCtx, rCommand, rv) -> {
         WriteCommand writeCommand = (WriteCommand) rCommand;
         if (!rCtx.isInTxScope()) {
            applyChanges(rCtx, writeCommand, null);
            return;
         }

         if (trace)
            log.tracef("The return value is %s", toStr(rv));
         if (useRepeatableRead) {
            boolean addVersionRead = writeSkewCheck && writeCommand.loadType() != VisitableCommand.LoadType.DONT_LOAD;
            TxInvocationContext txCtx = (TxInvocationContext) rCtx;
            for (Object key : writeCommand.getAffectedKeys()) {
               CacheEntry cacheEntry = rCtx.lookupEntry(key);
               if (cacheEntry != null) {
                  cacheEntry.setSkipLookup(true);
                  if (addVersionRead) {
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
   private BasicInvocationStage setSkipRemoteGetsAndInvokeNextForDataCommand(InvocationContext ctx,
         DataWriteCommand command, Metadata metadata) {
      return invokeNext(ctx, command).thenAccept((rCtx, rCommand, rv) -> {
         DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
         if (!rCtx.isInTxScope()) {
            applyChanges(rCtx, dataWriteCommand, metadata);
            return;
         }

         if (trace)
            log.tracef("The return value is %s", rv);
         if (useRepeatableRead) {
            CacheEntry cacheEntry = rCtx.lookupEntry(dataWriteCommand.getKey());
            // The entry is not in context when the command's execution type does not contain origin
            if (cacheEntry != null) {
               cacheEntry.setSkipLookup(true);
               if (writeSkewCheck && dataWriteCommand.loadType() != VisitableCommand.LoadType.DONT_LOAD) {
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
         boolean ignoreOwnership = ignoreOwnership(command);
         for (Object key : command.getAffectedKeys()) {
            entryFactory.wrapEntryForWriting(ctx, key, ignoreOwnership || canRead(key));
         }
         return invokeNext(ctx, command);
      }

      @Override
      public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
         boolean ignoreOwnership = ignoreOwnership(command);
         for (Object key : command.getAffectedKeys()) {
            entryFactory.wrapEntryForWriting(ctx, key, ignoreOwnership || canRead(key));
         }
         return invokeNext(ctx, command);
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), ignoreOwnership(command) || canRead(command.getKey()));
         return invokeNext(ctx, command);
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), ignoreOwnership(command) || canRead(command.getKey()));
         return invokeNext(ctx, command);
      }

      @Override
      public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
         entryFactory.wrapEntryForDelta(ctx, command.getKey(), command.getDelta(), ignoreOwnership(command) || canRead(command.getKey()));
         return invokeNext(ctx, command);
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), ignoreOwnership(command) || canRead(command.getKey()));
         return invokeNext(ctx, command);
      }
   }

   private boolean commitEntryIfNeeded(final InvocationContext ctx, final FlagAffectedCommand command,
         final CacheEntry entry, final Flag stateTransferFlag, final Metadata metadata) {
      if (entry == null) {
         return false;
      }
      final boolean l1Invalidation = command instanceof InvalidateL1Command;

      if (entry.isChanged()) {
         if (trace) log.tracef("About to commit entry %s", entry);
         commitContextEntry(entry, ctx, command, metadata, stateTransferFlag, l1Invalidation);

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
      boolean isTotalOrder = cacheConfiguration.transaction().transactionProtocol().isTotalOrder();
      return isTotalOrder ? command.isOnePhaseCommit() && (!ctx.isOriginLocal() || !command.hasModifications()) :
            command.isOnePhaseCommit();
   }

   protected final void wrapEntriesForPrepare(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!ctx.isOriginLocal() || command.isReplayEntryWrapping()) {
         for (WriteCommand c : command.getModifications()) {
            c.setTopologyId(command.getTopologyId());
            // TODO: we need async invocation since the interceptors may do remote get
            InvocationStage visitorStage = (InvocationStage) c.acceptVisitor(ctx, entryWrappingVisitor);
            if (visitorStage != null) {
               // Wait for the sub-command to finish. If there was an exception, rethrow it.
               visitorStage.get();
            }

            if (c.hasFlag(Flag.PUT_FOR_X_SITE_STATE_TRANSFER)) {
               ctx.getCacheTransaction().setStateTransferFlag(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
            }
         }
      }
   }
}
