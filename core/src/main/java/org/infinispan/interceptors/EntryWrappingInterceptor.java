package org.infinispan.interceptors;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
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
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.Metadata;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteStateConsumer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.infinispan.commons.util.Util.toStr;

/**
 * Interceptor in charge with wrapping entries and add them in caller's context.
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 * @since 5.1
 */
public class EntryWrappingInterceptor extends CommandInterceptor {

   private EntryFactory entryFactory;
   protected DataContainer dataContainer;
   protected ClusteringDependentLogic cdl;
   protected final EntryWrappingVisitor entryWrappingVisitor = new EntryWrappingVisitor();
   private CommandsFactory commandFactory;
   private boolean isUsingLockDelegation;
   private boolean isInvalidation;
   private StateConsumer stateConsumer;       // optional
   private StateTransferLock stateTransferLock;
   private XSiteStateConsumer xSiteStateConsumer;

   private static final Log log = LogFactory.getLog(EntryWrappingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(EntryFactory entryFactory, DataContainer dataContainer, ClusteringDependentLogic cdl,
                    CommandsFactory commandFactory, StateConsumer stateConsumer, StateTransferLock stateTransferLock,
                    XSiteStateConsumer xSiteStateConsumer) {
      this.entryFactory = entryFactory;
      this.dataContainer = dataContainer;
      this.cdl = cdl;
      this.commandFactory = commandFactory;
      this.stateConsumer = stateConsumer;
      this.stateTransferLock = stateTransferLock;
      this.xSiteStateConsumer = xSiteStateConsumer;
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
      try {
         entryFactory.wrapEntryForReading(ctx, command.getKey());
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
   public final Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      if (command.getKeys() != null) {
         for (Object key : command.getKeys()) {
            //for the invalidate command, we need to try to fetch the key from the data container
            //otherwise it may be not removed
            entryFactory.wrapEntryForRemove(ctx, key, false, true, false);
         }
      }
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, null);
   }

   @Override
   public final Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      for (InternalCacheEntry entry : dataContainer.entrySet())
         entryFactory.wrapEntryForClear(ctx, entry.getKey());
      return setSkipRemoteGetsAndInvokeNextForClear(ctx, command);
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      for (Object key : command.getKeys()) {
         //for the invalidate command, we need to try to fetch the key from the data container
         //otherwise it may be not removed
        entryFactory.wrapEntryForRemove(ctx, key, false, true, false);
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

   private void wrapEntryForPutIfNeeded(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (shouldWrap(command.getKey(), ctx, command)) {
         entryFactory.wrapEntryForPut(ctx, command.getKey(), null, !command.isConditional(), command,
                                      command.hasFlag(Flag.IGNORE_RETURN_VALUES) && !command.isConditional());
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
         // Should use wrapEntryForPut, but it doesn't work for now because of AtomicMap
         // AtomicMap put leaves a DeltaAwareCacheEntry in the context, and wrapEntryForPut
         // always expects a RepeatableReadEntry when repeatable read is enabled.
         entryFactory.wrapEntryForRemove(ctx, command.getKey(),
               command.hasFlag(Flag.IGNORE_RETURN_VALUES) && !command.isConditional(), false, forceWrap);
      }
   }

   @Override
   public final Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      wrapEntryForReplaceIfNeeded(ctx, command);
      return setSkipRemoteGetsAndInvokeNextForDataCommand(ctx, command, command.getMetadata());
   }

   private void wrapEntryForReplaceIfNeeded(InvocationContext ctx, ReplaceCommand command) throws InterruptedException {
      if (shouldWrap(command.getKey(), ctx, command)) {
         if (command.getValueMatcher().nonExistentEntryCanMatch()) {
            // wrap it for put, as the previous value might not be present by now (e.g. might have been deleted)
            // but we still need to apply the new value.
            entryFactory.wrapEntryForPut(ctx, command.getKey(), null, false, command, false);
         } else  {
            entryFactory.wrapEntryForReplace(ctx, command);
         }
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      for (Object key : command.getMap().keySet()) {
         if (shouldWrap(key, ctx, command)) {
            //the put map never reads the keys
            entryFactory.wrapEntryForPut(ctx, key, null, true, command, true);
         }
      }
      return setSkipRemoteGetsAndInvokeNextForPutMapCommand(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      command.setFlags(Flag.SKIP_OWNERSHIP_CHECK, Flag.CACHE_MODE_LOCAL); //to force the wrapping
      return visitRemoveCommand(ctx, command);
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

   private boolean hasClearCommand(InvocationContext ctx, FlagAffectedCommand command) {
      return ctx instanceof TxInvocationContext ?
            ((TxInvocationContext) ctx).getCacheTransaction().hasModification(ClearCommand.class) :
            command instanceof ClearCommand;
   }

   protected final void commitContextEntries(InvocationContext ctx, FlagAffectedCommand command, Metadata metadata) {
      final Flag stateTransferFlag = extractStateTransferFlag(ctx, command);

      if (stateTransferFlag == null) {
         //it is a normal operation
         stopStateTransferIfNeeded(ctx, command);
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

   private void stopStateTransferIfNeeded(InvocationContext context, FlagAffectedCommand command) {
      if (hasClearCommand(context, command)) {
         // If we are committing a ClearCommand now then no keys should be written by state transfer from
         // now on until current rebalance ends.
         if (stateConsumer != null) {
            stateConsumer.stopApplyingState();
         }
         if (xSiteStateConsumer != null) {
            xSiteStateConsumer.endStateTransfer();
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
   private Object setSkipRemoteGetsAndInvokeNextForClear(InvocationContext context, ClearCommand command) throws Throwable {
      boolean txScope = context.isInTxScope();
      if (txScope) {
         for (CacheEntry entry : context.getLookedUpEntries().values()) {
            if (entry != null) {
               entry.setSkipLookup(true);
            }
         }
      }
      Object retVal = invokeNextAndApplyChanges(context, command, command.getMetadata());
      if (txScope) {
         for (CacheEntry entry : context.getLookedUpEntries().values()) {
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
   private Object setSkipRemoteGetsAndInvokeNextForPutMapCommand(InvocationContext context, PutMapCommand command) throws Throwable {
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
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         boolean wrapped = false;
         for (Object key : dataContainer.keySet()) {
            entryFactory.wrapEntryForClear(ctx, key);
            wrapped = true;
         }
         if (wrapped)
            invokeNextInterceptor(ctx, command);
         if (stateConsumer != null && !ctx.isInTxScope()) {
            // If a non-tx ClearCommand was executed successfully we must stop recording updated keys and do not
            // allow any further updates to be written by state transfer from now on until current rebalance ends.
            stateConsumer.stopApplyingState();
         }
         return null;
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         Map<Object, Object> newMap = new HashMap<Object, Object>(4);
         for (Map.Entry<Object, Object> e : command.getMap().entrySet()) {
            Object key = e.getKey();
            if (cdl.localNodeIsOwner(key)) {
               entryFactory.wrapEntryForPut(ctx, key, null, true, command, false);
               newMap.put(key, e.getValue());
            }
         }
         if (newMap.size() > 0) {
            PutMapCommand clonedCommand = commandFactory.buildPutMapCommand(newMap,
                  command.getMetadata(), command.getFlags());
            invokeNextInterceptor(ctx, clonedCommand);
         }
         return null;
      }

      @Override
      public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
         if (command.getKeys() != null) {
            for (Object key : command.getKeys()) {
               if (cdl.localNodeIsOwner(key)) {
                  entryFactory.wrapEntryForRemove(ctx, key, false, false, false);
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
            entryFactory.wrapEntryForRemove(ctx, command.getKey(), false, false, forceWrap);
            invokeNextInterceptor(ctx, command);
         }
         return null;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (cdl.localNodeIsOwner(command.getKey())) {
            entryFactory.wrapEntryForPut(ctx, command.getKey(), null, !command.isConditional(), command, false);
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
            if (command.getValueMatcher().nonExistentEntryCanMatch()) {
               //wrap it for put, as the previous value might not be present by now (e.g. might have been deleted)
               // but we still need to apply the new value.
               entryFactory.wrapEntryForPut(ctx, command.getKey(), null, false, command, false);
            } else  {
               entryFactory.wrapEntryForReplace(ctx, command);
            }
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

      if (entry.isChanged() || entry.isLoaded()) {
         log.tracef("About to commit entry %s", entry);
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
