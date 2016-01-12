package org.infinispan.interceptors;

import static org.infinispan.commons.util.Util.toStr;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.functional.*;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.*;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.NullCacheEntry;
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
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.Metadata;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteStateConsumer;

/**
 * Interceptor in charge with wrapping entries and add them in caller's context.
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public class EntryWrappingInterceptor extends CommandInterceptor {

   private EntryFactory entryFactory;
   protected DataContainer<Object, Object> dataContainer;
   protected ClusteringDependentLogic cdl;
   protected final EntryWrappingVisitor entryWrappingVisitor = new EntryWrappingVisitor();
   private CommandsFactory commandFactory;
   private boolean isUsingLockDelegation;
   private boolean isInvalidation;
   private StateConsumer stateConsumer;       // optional
   private StateTransferLock stateTransferLock;
   private XSiteStateConsumer xSiteStateConsumer;
   private GroupManager groupManager;

   private static final Log log = LogFactory.getLog(EntryWrappingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final EnumSet<Flag> EVICT_FLAGS =
         EnumSet.of(Flag.SKIP_OWNERSHIP_CHECK, Flag.CACHE_MODE_LOCAL);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(EntryFactory entryFactory, DataContainer<Object, Object> dataContainer, ClusteringDependentLogic cdl,
                    CommandsFactory commandFactory, StateConsumer stateConsumer, StateTransferLock stateTransferLock,
                    XSiteStateConsumer xSiteStateConsumer, GroupManager groupManager) {
      this.entryFactory = entryFactory;
      this.dataContainer = dataContainer;
      this.cdl = cdl;
      this.commandFactory = commandFactory;
      this.stateConsumer = stateConsumer;
      this.stateTransferLock = stateTransferLock;
      this.xSiteStateConsumer = xSiteStateConsumer;
      this.groupManager = groupManager;
   }

   @Start
   public void start() {
      isUsingLockDelegation = !cacheConfiguration.transaction().transactionMode().isTransactional() &&
            (cacheConfiguration.clustering().cacheMode().isDistributed() ||
                   cacheConfiguration.clustering().cacheMode().isReplicated());
      isInvalidation = cacheConfiguration.clustering().cacheMode().isInvalidation();
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      wrapEntriesForPrepare(ctx, command);
      Object result = invokeNextInterceptor(ctx, command);
      if (shouldCommitDuringPrepare(command, ctx)) {
         commitContextEntries(ctx, null, null);
      }
      return result;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         commitContextEntries(ctx, null, null);
      }
   }

   @Override
   public final Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }
   @Override
   public final Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   private Object visitDataReadCommand(InvocationContext ctx, AbstractDataCommand command) throws Throwable {
      try {
         entryFactory.wrapEntryForReading(ctx, command.getKey(), null);
         return invokeNextInterceptor(ctx, command);
      } finally {
         //needed because entries might be added in L1
         if (!ctx.isInTxScope())
            commitContextEntries(ctx, command, null);
         else {
            CacheEntry entry = ctx.lookupEntry(command.getKey());
            if (entry != null) {
               entry.setSkipLookup(true);
            }
         }
      }
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      try {
         for (Object key : command.getKeys()) {
            entryFactory.wrapEntryForReading(ctx, key, null);
         }
         return invokeNextInterceptor(ctx, command);
      } finally {
         if (ctx.isInTxScope()) {
            for (Object key : command.getKeys()) {
               CacheEntry entry = ctx.lookupEntry(key);
               if (entry != null) {
                  entry.setSkipLookup(true);
               }
            }
         }
      }
   }

   @Override
   public final Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      if (command.getKeys() != null) {
         for (Object key : command.getKeys()) {
            //for the invalidate command, we need to try to fetch the key from the data container
            //otherwise it may be not removed
            entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_NON_NULL, false, true);
         }
      }
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, null);
   }

   @Override
   public final Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNextAndApplyChanges(ctx, command, command.getMetadata());
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      for (Object key : command.getKeys()) {
         //for the invalidate command, we need to try to fetch the key from the data container
         //otherwise it may be not removed
         entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_NON_NULL, false, true);
         if (trace)
           log.tracef("Entry to be removed: %s", toStr(key));
      }
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, null);
   }

   @Override
   public final Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      wrapEntryForPutIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, command.getMetadata());
   }

   private void wrapEntryForPutIfNeeded(InvocationContext ctx, AbstractDataWriteCommand command) throws Throwable {
      if (shouldWrap(command.getKey(), ctx, command)) {
         boolean skipRead = command.hasFlag(Flag.IGNORE_RETURN_VALUES) && !command.isConditional();
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), EntryFactory.Wrap.WRAP_ALL, skipRead, false);
      }
   }

   private boolean shouldWrap(Object key, InvocationContext ctx, FlagAffectedCommand command) {
      if (command.hasFlag(Flag.SKIP_OWNERSHIP_CHECK)) {
         if (trace)
            log.tracef("Skipping ownership check and wrapping key %s", toStr(key));

         return true;
      } else if (command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (trace) {
            log.tracef("CACHE_MODE_LOCAL is set. Wrapping key %s", toStr(key));
         }
         return true;
      }
      boolean result;
      boolean isTransactional = cacheConfiguration.transaction().transactionMode().isTransactional();
      boolean isPutForExternalRead = command.hasFlag(Flag.PUT_FOR_EXTERNAL_READ);

      // Invalidated caches should always wrap entries in order to local
      // changes from nodes that are not lock owners for these entries.
      // Switching ClusteringDependentLogic to handle this, i.e.
      // localNodeIsPrimaryOwner to always return true, would have had negative
      // impact on locking since locks would be always be acquired locally
      // and that would lead to deadlocks.
      if (isInvalidation || (isTransactional && !isPutForExternalRead)) {
         result = true;
      } else {
         if (isUsingLockDelegation || isTransactional) {
            result = cdl.localNodeIsPrimaryOwner(key) || (cdl.localNodeIsOwner(key) && !ctx.isOriginLocal());
         } else {
            result = cdl.localNodeIsOwner(key);
         }
      }

      if (trace)
         log.tracef("Wrapping entry '%s'? %s", toStr(key), result);

      return result;
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      entryFactory.wrapEntryForDelta(ctx, command.getKey(), command.getDelta());
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public final Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      wrapEntryForRemoveIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, null);
   }

   private void wrapEntryForRemoveIfNeeded(InvocationContext ctx, RemoveCommand command) throws InterruptedException {
      if (shouldWrap(command.getKey(), ctx, command)) {
         boolean forceWrap = command.getValueMatcher().nonExistentEntryCanMatch();
         EntryFactory.Wrap wrap = forceWrap ? EntryFactory.Wrap.WRAP_ALL : EntryFactory.Wrap.WRAP_NON_NULL;
         boolean skipRead = command.hasFlag(Flag.IGNORE_RETURN_VALUES) && !command.isConditional();
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), wrap, skipRead, false);
      }
   }

   @Override
   public final Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      wrapEntryForReplaceIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, command.getMetadata());
   }

   private void wrapEntryForReplaceIfNeeded(InvocationContext ctx, ReplaceCommand command) throws InterruptedException {
      if (shouldWrap(command.getKey(), ctx, command)) {
         // When retrying, we might still need to perform the command even if the previous value was removed
         EntryFactory.Wrap wrap =
               command.getValueMatcher().nonExistentEntryCanMatch() ? EntryFactory.Wrap.WRAP_ALL :
               EntryFactory.Wrap.WRAP_NON_NULL;
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), wrap, false, false);
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      for (Object key : command.getMap().keySet()) {
         if (shouldWrap(key, ctx, command)) {
            //the put map never reads the keys
            entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_ALL, true, false);
         }
      }
      return setSkipRemoteGetsAndInvokeNextForPutMapCommand(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      command.addFlags(EVICT_FLAGS); //to force the wrapping
      return visitRemoveCommand(ctx, command);
   }

   @Override
   public Object visitGetKeysInGroupCommand(final InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
      final String groupName = command.getGroupName();
      if (!command.isGroupOwner()) {
         return invokeNextInterceptor(ctx, command);
      }
      final KeyFilter<Object> keyFilter = new CompositeKeyFilter<>(new GroupFilter<>(groupName, groupManager),
                                                                   new CollectionKeyFilter<>(ctx.getLookedUpEntries().keySet()));
      dataContainer.executeTask(keyFilter, (o, internalCacheEntry) -> {
         synchronized (ctx) {
            //the process can be made in multiple threads, so we need to synchronize in the context.
            entryFactory.wrapExternalEntry(ctx, internalCacheEntry.getKey(), internalCacheEntry,
                                           EntryFactory.Wrap.STORE, false);
         }
      });
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      try {
         CacheEntry entry = entryFactory.wrapEntryForReading(ctx, command.getKey(), null);
         // Null entry is often considered to mean that entry is not available
         // locally, but if there's no need to get remote, the read-only
         // function needs to be executed, so force a non-null entry in
         // context with null content
         if (entry == null && cdl.localNodeIsOwner(command.getKey())) {
            entryFactory.wrapEntryForReading(ctx, command.getKey(), NullCacheEntry.getInstance());
         }

         return invokeNextInterceptor(ctx, command);
      } finally {
         //needed because entries might be added in L1
         if (!ctx.isInTxScope())
            commitContextEntries(ctx, command, null);
         else {
            CacheEntry entry = ctx.lookupEntry(command.getKey());
            if (entry != null) {
               entry.setSkipLookup(true);
            }
         }
      }
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      try {
         for (Object key : command.getKeys()) {
            entryFactory.wrapEntryForReading(ctx, key, null);
         }
         return invokeNextInterceptor(ctx, command);
      } finally {
         if (ctx.isInTxScope()) {
            for (Object key : command.getKeys()) {
               CacheEntry entry = ctx.lookupEntry(key);
               if (entry != null) {
                  entry.setSkipLookup(true);
               }
            }
         }
      }
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      wrapEntryForPutIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, command.getMetadata());
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      wrapEntryForPutIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, command.getMetadata());
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      wrapEntryForPutIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, command.getMetadata());
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      for (Object key : command.getEntries().keySet()) {
         if (shouldWrap(key, ctx, command)) {
            //the put map never reads the keys
            entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_ALL, true, false);
         }
      }
      return setSkipRemoteGetsAndInvokeNextForPutMapCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      for (Object key : command.getKeys()) {
         if (shouldWrap(key, ctx, command)) {
            //the put map never reads the keys
            entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_ALL, true, false);
         }
      }
      return setSkipRemoteGetsAndInvokeNextForPutMapCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      wrapEntryForPutIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, command.getMetadata());
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      for (Object key : command.getKeys()) {
         if (shouldWrap(key, ctx, command)) {
            entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_ALL, false, false);
         }
      }
      return setSkipRemoteGetsAndInvokeNextForPutMapCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      for (Object key : command.getEntries().keySet()) {
         if (shouldWrap(key, ctx, command)) {
            entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_ALL, false, false);
         }
      }
      return setSkipRemoteGetsAndInvokeNextForPutMapCommand(ctx, command);
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

      if (stateTransferFlag == null) {
         //it is a normal operation
         stopStateTransferIfNeeded(command);
      }

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
                     log.tracef("Entry for key %s is null : not calling commitUpdate", e.getKey());
                  else
                     log.tracef("Entry for key %s is not changed(%s): not calling commitUpdate", e.getKey(), entry);
               }
            }
         }
      }
   }

   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, FlagAffectedCommand command,
                                     Metadata metadata, Flag stateTransferFlag, boolean l1Invalidation) {
      cdl.commitEntry(entry, metadata, command, ctx, stateTransferFlag, l1Invalidation);
   }

   private void stopStateTransferIfNeeded(FlagAffectedCommand command) {
      if (command instanceof ClearCommand) {
         // If we are committing a ClearCommand now then no keys should be written by state transfer from
         // now on until current rebalance ends.
         if (stateConsumer != null) {
            stateConsumer.stopApplyingState();
         }
         if (xSiteStateConsumer != null) {
            xSiteStateConsumer.endStateTransfer(null);
         }
      }
   }

   private Object invokeNextAndApplyChanges(InvocationContext ctx, FlagAffectedCommand command, Metadata metadata) throws Throwable {
      final Object result = invokeNextInterceptor(ctx, command);

      if (!ctx.isInTxScope()) {
         stateTransferLock.acquireSharedTopologyLock();
         try {
            // We only retry non-tx write commands
            if (command instanceof WriteCommand) {
               WriteCommand writeCommand = (WriteCommand) command;
               // Can't perform the check during preload or if the cache isn't clustered
               boolean isSync = (cacheConfiguration.clustering().cacheMode().isSynchronous() &&
                     !command.hasFlag(Flag.FORCE_ASYNCHRONOUS)) || command.hasFlag(Flag.FORCE_SYNCHRONOUS);
               if (writeCommand.isSuccessful() && stateConsumer != null &&
                     stateConsumer.getCacheTopology() != null) {
                  int commandTopologyId = command.getTopologyId();
                  int currentTopologyId = stateConsumer.getCacheTopology().getTopologyId();
                  // TotalOrderStateTransferInterceptor doesn't set the topology id for PFERs.
                  if (isSync && currentTopologyId != commandTopologyId && commandTopologyId != -1) {
                     // If we were the originator of a data command which we didn't own the key at the time means it
                     // was already committed, so there is no need to throw the OutdatedTopologyException
                     // This will happen if we submit a command to the primary owner and it responds and then a topology
                     // change happens before we get here
                     if (!ctx.isOriginLocal() || !(command instanceof DataCommand) ||
                               ctx.hasLockedKey(((DataCommand)command).getKey())) {
                        if (trace) log.tracef("Cache topology changed while the command was executing: expected %d, got %d",
                              commandTopologyId, currentTopologyId);
                        // This shouldn't be necessary, as we'll have a fresh command instance when retrying
                        writeCommand.setValueMatcher(writeCommand.getValueMatcher().matcherForRetry());
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

      if (trace) log.tracef("The return value is %s", result);
      return result;
   }

   /**
    * Locks the value for the keys accessed by the command to avoid being override from a remote get.
    */
   private Object setSkipRemoteGetsAndInvokeNextForPutMapCommand(InvocationContext context, WriteCommand command) throws Throwable {
      Object retVal = invokeNextAndApplyChanges(context, command, command.getMetadata());
      if (context.isInTxScope()) {
         for (Object key : command.getAffectedKeys()) {
            CacheEntry entry = context.lookupEntry(key);
            if (entry != null) {
               entry.setSkipLookup(true);
            }
         }
      }
      return retVal;
   }

   /**
    * Locks the value for the keys accessed by the command to avoid being override from a remote get.
    */
   private Object setSkipRemoteGetsAndInvokeNextForDataCommand(InvocationContext context, DataWriteCommand command,
                                                               Metadata metadata) throws Throwable {
      Object retVal = invokeNextAndApplyChanges(context, command, metadata);
      if (context.isInTxScope()) {
         CacheEntry entry = context.lookupEntry(command.getKey());
         if (entry != null) {
            entry.setSkipLookup(true);
         }
      }
      return retVal;
   }

   private final class EntryWrappingVisitor extends AbstractVisitor {

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         Map<Object, Object> newMap = new HashMap<>(4);
         for (Map.Entry<Object, Object> e : command.getMap().entrySet()) {
            Object key = e.getKey();
            if (cdl.localNodeIsOwner(key)) {
               entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_ALL, true, false);
               newMap.put(key, e.getValue());
            }
         }
         if (newMap.size() > 0) {
            PutMapCommand clonedCommand = commandFactory.buildPutMapCommand(newMap,
                  command.getMetadata(), command.getFlagsBitSet());
            invokeNextInterceptor(ctx, clonedCommand);
         }
         return null;
      }

      @Override
      public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
         if (command.getKeys() != null) {
            for (Object key : command.getKeys()) {
               if (cdl.localNodeIsOwner(key)) {
                  entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_NON_NULL, false, false);
                  invokeNextInterceptor(ctx, command);
               }
            }
         }
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         if (cdl.localNodeIsOwner(command.getKey())) {
            boolean forceWrap = command.getValueMatcher().nonExistentEntryCanMatch();
            EntryFactory.Wrap wrap = forceWrap ? EntryFactory.Wrap.WRAP_ALL : EntryFactory.Wrap.WRAP_NON_NULL;
            entryFactory.wrapEntryForWriting(ctx, command.getKey(), wrap, false, false);
            invokeNextInterceptor(ctx, command);
         }
         return null;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (cdl.localNodeIsOwner(command.getKey())) {
            entryFactory.wrapEntryForWriting(ctx, command.getKey(), EntryFactory.Wrap.WRAP_ALL, false, false);
            invokeNextInterceptor(ctx, command);
         }
         return null;
      }

      @Override
      public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
         if (cdl.localNodeIsOwner(command.getKey())) {
            entryFactory.wrapEntryForDelta(ctx, command.getKey(), command.getDelta());
            invokeNextInterceptor(ctx, command);
         }
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         if (cdl.localNodeIsOwner(command.getKey())) {
            // When retrying, we need to perform the command even if the previous value was deleted.
            boolean forceWrap = command.getValueMatcher().nonExistentEntryCanMatch();
            EntryFactory.Wrap wrap = forceWrap ? EntryFactory.Wrap.WRAP_ALL : EntryFactory.Wrap.WRAP_NON_NULL;
            entryFactory.wrapEntryForWriting(ctx, command.getKey(), wrap, false, false);
            invokeNextInterceptor(ctx, command);
         }
         return null;
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
            c.acceptVisitor(ctx, entryWrappingVisitor);
            if (c.hasFlag(Flag.PUT_FOR_X_SITE_STATE_TRANSFER)) {
               ctx.getCacheTransaction().setStateTransferFlag(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
            }
         }
      }
   }
}
