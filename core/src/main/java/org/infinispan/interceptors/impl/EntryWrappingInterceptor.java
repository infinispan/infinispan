package org.infinispan.interceptors.impl;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
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
import java.util.concurrent.CompletableFuture;

import static org.infinispan.commons.util.Util.toStr;

/**
 * Interceptor in charge with wrapping entries and add them in caller's context.
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 * @since 9.0
 */
public class EntryWrappingInterceptor extends BaseEntryWrappingInterceptor {
   protected final EntryWrappingVisitor entryWrappingVisitor = new EntryWrappingVisitor();
   private CommandsFactory commandFactory;
   private StateConsumer stateConsumer;       // optional
   private StateTransferLock stateTransferLock;
   private XSiteStateConsumer xSiteStateConsumer;

   private static final Log log = LogFactory.getLog(EntryWrappingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ReturnHandler dataReadReturnHandler = new ReturnHandler() {
      @Override
      public CompletableFuture<Object> handle(InvocationContext rCtx, VisitableCommand rCommand, Object rv,
            Throwable throwable) throws Throwable {
         AbstractDataCommand command1 = (AbstractDataCommand) rCommand;

         // TODO needed because entries might be added in L1?
         if (!rCtx.isInTxScope()) {
            commitContextEntries(rCtx, command1, null);
         } else {
            setSkipLookup(rCtx, command1.getKey());
         }

         // Entry visit notifications used to happen in the CallInterceptor
         // We do it after (maybe) committing the entries, to avoid adding another try/finally block
         if (throwable == null && rv != null) {
            Object value = command1 instanceof GetCacheEntryCommand ? ((CacheEntry) rv).getValue() : rv;
            notifier.notifyCacheEntryVisited(command1.getKey(), value, true, rCtx, command1);
            notifier.notifyCacheEntryVisited(command1.getKey(), value, false, rCtx, command1);
         }
         return null;
      }
   };

   private final ReturnHandler commitEntriesOnSuccessReturnHandler = new ReturnHandler() {
      @Override
      public CompletableFuture<Object> handle(InvocationContext rCtx, VisitableCommand rCommand, Object rv,
            Throwable throwable) throws Throwable {
         if (throwable == null) {
            commitContextEntries(rCtx, null, null);
         }
         return null;
      }
   };

   private final ReturnHandler commitEntriesReturnHandler = new ReturnHandler() {
      @Override
      public CompletableFuture<Object> handle(InvocationContext rCtx, VisitableCommand rCommand, Object rv,
            Throwable throwable) throws Throwable {
         commitContextEntries(rCtx, null, null);
         return null;
      }
   };

   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(CommandsFactory commandFactory, StateConsumer stateConsumer,
                    StateTransferLock stateTransferLock, XSiteStateConsumer xSiteStateConsumer) {
      this.commandFactory = commandFactory;
      this.stateConsumer = stateConsumer;
      this.stateTransferLock = stateTransferLock;
      this.xSiteStateConsumer = xSiteStateConsumer;
   }

   @Override
   public CompletableFuture<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      wrapEntriesForPrepare(ctx, command);
      if (!shouldCommitDuringPrepare(command, ctx)) {
         return ctx.continueInvocation();
      }
      return ctx.onReturn(commitEntriesOnSuccessReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return ctx.onReturn(commitEntriesReturnHandler);
   }

   @Override
   protected CompletableFuture<Void> visitDataReadCommand(InvocationContext ctx, AbstractDataCommand command) throws Throwable {
      ctx.onReturn(dataReadReturnHandler);
      entryFactory.wrapEntryForReading(ctx, command.getKey(), null, command.hasFlag(Flag.SKIP_OWNERSHIP_CHECK));
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      for (Object key : command.getKeys()) {
         entryFactory.wrapEntryForReading(ctx, key, null, command.hasFlag(Flag.SKIP_OWNERSHIP_CHECK));
      }
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         GetAllCommand getAllCommand = (GetAllCommand) rCommand;
         if (ctx.isInTxScope()) {
            for (Object key : getAllCommand.getKeys()) {
               setSkipLookup(rCtx, key);
            }
         }

         // Entry visit notifications used to happen in the CallInterceptor
         if (throwable == null && rv != null) {
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
         return null;
      });
   }

   private void setSkipLookup(InvocationContext ctx, Object key) {
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry != null) {
         entry.setSkipLookup(true);
      }
   }

   /**
    * Locks the value for the keys accessed by the command to avoid being override from a remote get.
    */
   @Override
   protected CompletableFuture<Void> handleDataWriteReturn(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable != null)
            throw throwable;

         DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
         if (!rCtx.isInTxScope()) {
            applyChanges(rCtx, dataWriteCommand, dataWriteCommand.getMetadata());
         }

         if (trace)
            log.tracef("The return value is %s", rv);
         if (rCtx.isInTxScope()) {
            setSkipLookup(rCtx, dataWriteCommand.getKey());
         }
         return null;
      });
   }

   /**
    * Locks the value for the keys accessed by the command to avoid being override from a remote get.
    */
   @Override
   protected CompletableFuture<Void> handleManyWriteReturn(InvocationContext ctx) throws Throwable {
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable != null)
            throw throwable;

         WriteCommand writeCommand = (WriteCommand) rCommand;
         if (!rCtx.isInTxScope()) {
            applyChanges(rCtx, writeCommand, writeCommand.getMetadata());
         }

         if (trace)
            log.tracef("The return value is %s", toStr(rv));
         if (rCtx.isInTxScope()) {
            for (Object key : writeCommand.getAffectedKeys()) {
               setSkipLookup(rCtx, key);
            }
         }
         return null;
      });
   }

   @Override
   public final CompletableFuture<Void> visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      if (command.getKeys() != null) {
         for (Object key : command.getKeys()) {
            //for the invalidate command, we need to try to fetch the key from the data container
            //otherwise it may be not removed
            entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_NON_NULL, false, true);
         }
      }
      return handleManyWriteReturn(ctx);
   }

   @Override
   public final CompletableFuture<Void> visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         // If we are committing a ClearCommand now then no keys should be written by state transfer from
         // now on until current rebalance ends.
         if (stateConsumer != null) {
            stateConsumer.stopApplyingState(stateConsumer.getCacheTopology().getTopologyId());
         }
         if (xSiteStateConsumer != null) {
            xSiteStateConsumer.endStateTransfer(null);
         }

         if (!rCtx.isInTxScope()) {
            ClearCommand clearCommand = (ClearCommand) rCommand;
            applyChanges(rCtx, clearCommand, clearCommand.getMetadata());
         }

         if (trace)
            log.tracef("The return value is %s", rv);
         return null;
      });
   }

   @Override
   public CompletableFuture<Void> visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      for (Object key : command.getKeys()) {
         //for the invalidate command, we need to try to fetch the key from the data container
         //otherwise it may be not removed
         entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_NON_NULL, false, true);
         if (trace)
           log.tracef("Entry to be removed: %s", toStr(key));
      }
      return handleDataWriteReturn(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      super.visitReadOnlyKeyCommand(ctx, command);
      //needed because entries might be added in L1
      if (!ctx.isInTxScope())
         return ctx.onReturn(commitEntriesReturnHandler);
      else {
         return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
            setSkipLookup(rCtx, ((ReadOnlyKeyCommand) rCommand).getKey());
            return null;
         });
      }
   }

   @Override
   public CompletableFuture<Void> visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      super.visitReadOnlyManyCommand(ctx, command);
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         for (Object key : ((ReadOnlyManyCommand) rCommand).getKeys()) {
            setSkipLookup(rCtx, key);
         }
         return null;
      });
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

   private void applyChanges(InvocationContext ctx, FlagAffectedCommand command, Metadata metadata) {
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

   // This visitor replays the entry wrapping during remote prepare.
   // Remote writes never request the previous value from a different node,
   // so it should be safe to keep this synchronous.
   private final class EntryWrappingVisitor extends AbstractVisitor {
      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         Map<Object, Object> newMap = new HashMap<>(4);
         for (Map.Entry<Object, Object> e : command.getMap().entrySet()) {
            Object key = e.getKey();
            if (cdl.canWriteEntry(key)) {
               entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_ALL, true, false);
               newMap.put(key, e.getValue());
            }
         }
         if (newMap.size() > 0) {
            PutMapCommand clonedCommand = commandFactory.buildPutMapCommand(newMap,
                  command.getMetadata(), command.getFlagsBitSet());
            ctx.forkInvocationSync(clonedCommand);
         }
         return null;
      }

      @Override
      public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
         if (command.getKeys() != null) {
            for (Object key : command.getKeys()) {
               if (cdl.canWriteEntry(key)) {
                  entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_NON_NULL, false, false);
                  ctx.forkInvocationSync(command);
               }
            }
         }
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         if (cdl.canWriteEntry(command.getKey())) {
            boolean forceWrap = command.getValueMatcher().nonExistentEntryCanMatch();
            EntryFactory.Wrap wrap = forceWrap ? EntryFactory.Wrap.WRAP_ALL : EntryFactory.Wrap.WRAP_NON_NULL;
            entryFactory.wrapEntryForWriting(ctx, command.getKey(), wrap, false, false);
            ctx.forkInvocationSync(command);
         }
         return null;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (cdl.canWriteEntry(command.getKey())) {
            entryFactory.wrapEntryForWriting(ctx, command.getKey(), EntryFactory.Wrap.WRAP_ALL, false, false);
            ctx.forkInvocationSync(command);
         }
         return null;
      }

      @Override
      public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
         if (cdl.canWriteEntry(command.getKey())) {
            entryFactory.wrapEntryForDelta(ctx, command.getKey(), command.getDelta());
            ctx.forkInvocationSync(command);
         }
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         if (cdl.canWriteEntry(command.getKey())) {
            // When retrying, we need to perform the command even if the previous value was deleted.
            boolean forceWrap = command.getValueMatcher().nonExistentEntryCanMatch();
            EntryFactory.Wrap wrap = forceWrap ? EntryFactory.Wrap.WRAP_ALL : EntryFactory.Wrap.WRAP_NON_NULL;
            entryFactory.wrapEntryForWriting(ctx, command.getKey(), wrap, false, false);
            ctx.forkInvocationSync(command);
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
