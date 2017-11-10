package org.infinispan.interceptors.distribution;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

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
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.L1Manager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.impl.BaseRpcInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * L1 based interceptor that flushes the L1 cache at the end after a transaction/entry is committed to the data
 * container but before the lock has been released.  This is here to asynchronously clear any L1 cached values that were
 * retrieved between when the data was updated, causing a L1 invalidation, and when the data was put into the data
 * container
 *
 * @author wburns
 */
public class L1LastChanceInterceptor extends BaseRpcInterceptor {

   private static final Log log = LogFactory.getLog(L1LastChanceInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject private L1Manager l1Manager;
   @Inject private ClusteringDependentLogic cdl;

   private final InvocationSuccessFunction handleDataWriteCommandEntryInL1 = this::handleDataWriteCommandEntryInL1;
   private final InvocationSuccessFunction handleDataWriteCommandEntryNotInL1 = this::handleDataWriteCommandEntryNotInL1;
   private final InvocationSuccessFunction handleWriteManyCommand = this::handleWriteManyCommand;
   private final InvocationSuccessFunction handlePrepareCommand = this::handlePrepareCommand;
   private final InvocationSuccessFunction handleCommitCommand = this::handleCommitCommand;

   private boolean nonTransactional;

   @Start
   public void start() {
      nonTransactional = !cacheConfiguration.transaction().transactionMode().isTransactional();
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command, true);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command, true);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command, false);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command, false);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command, false);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command, false);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command, false);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command, false);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleWriteManyCommand);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command, false);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleWriteManyCommand);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleWriteManyCommand);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleWriteManyCommand);
   }

   public Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command, boolean assumeOriginKeptEntryInL1) throws Throwable {
      return invokeNextThenApply(ctx, command, assumeOriginKeptEntryInL1 ? handleDataWriteCommandEntryInL1 : handleDataWriteCommandEntryNotInL1);
   }

   private Object handleDataWriteCommand(InvocationContext rCtx, VisitableCommand rCommand, Object rv, boolean assumeOriginKeptEntryInL1) {
      Object key;
      DataWriteCommand writeCommand = (DataWriteCommand) rCommand;
      Object key1 = (key = writeCommand.getKey());
      if (shouldUpdateOnWriteCommand(writeCommand) && writeCommand.isSuccessful() &&
            cdl.getCacheTopology().isWriteOwner(key1)) {
         if (trace) {
            log.trace("Sending additional invalidation for requestors if necessary.");
         }
         // Send out a last attempt L1 invalidation in case if someone cached the L1
         // value after they already received an invalidation
         CompletableFuture<?> f = l1Manager.flushCache(Collections.singleton(key), rCtx.getOrigin(), assumeOriginKeptEntryInL1);
         return asyncReturnValue(f, rv);
      }
      return rv;
   }

   private Object handleDataWriteCommandEntryInL1(InvocationContext rCtx, VisitableCommand rCommand, Object rv) {
      return handleDataWriteCommand(rCtx, rCommand, rv, true);
   }

   private Object handleDataWriteCommandEntryNotInL1(InvocationContext rCtx, VisitableCommand rCommand, Object rv) {
      return handleDataWriteCommand(rCtx, rCommand, rv, false);
   }

   private Object asyncReturnValue(CompletableFuture<?> f, Object rv) {
      if (f == null || f.isDone()) {
         return rv;
      }
      return asyncValue(f.handle((nil, throwable) -> {
         if (throwable != null) {
            getLog().failedInvalidatingRemoteCache(throwable);
            throw CompletableFutures.asCompletionException(throwable);
         }
         return rv;
      }));
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleWriteManyCommand);
   }

   private Object handleWriteManyCommand(InvocationContext rCtx, VisitableCommand rCommand, Object rv) {
      WriteCommand command = (WriteCommand) rCommand;
      if (shouldUpdateOnWriteCommand(command)) {
         Collection<?> keys = command.getAffectedKeys();
         Set<Object> toInvalidate = new HashSet<>(keys.size());
         for (Object k : keys) {
            if (cdl.getCacheTopology().isWriteOwner(k)) {
               toInvalidate.add(k);
            }
         }
         if (!toInvalidate.isEmpty()) {
            if (trace) {
               log.trace("Sending additional invalidation for requestors if necessary.");
            }
            CompletableFuture<?> f = l1Manager.flushCache(toInvalidate, rCtx.getOrigin(), true);
            return asyncReturnValue(f, rv);
         }
      }
      return rv;
   }

   private boolean shouldUpdateOnWriteCommand(WriteCommand command) {
      return nonTransactional && !command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handlePrepareCommand);
   }

   private Object handlePrepareCommand(InvocationContext rCtx, VisitableCommand rCommand, Object rv) {
      if (((PrepareCommand) rCommand).isOnePhaseCommit()) {
         CompletableFuture<?> f = handleLastChanceL1InvalidationOnCommit(((TxInvocationContext<?>) rCtx));
         return asyncReturnValue(f, rv);
      }
      return rv;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleCommitCommand);
   }

   private Object handleCommitCommand(InvocationContext rCtx, VisitableCommand rCommand, Object rv) {
      CompletableFuture<?> f = handleLastChanceL1InvalidationOnCommit((TxInvocationContext<?>) rCtx);
      return asyncReturnValue(f, rv);
   }

   private CompletableFuture<?> handleLastChanceL1InvalidationOnCommit(TxInvocationContext<?> ctx) {
      if (shouldFlushL1(ctx)) {
         if (trace) {
            log.tracef("Sending additional invalidation for requestors if necessary.");
         }
         return l1Manager.flushCache(ctx.getAffectedKeys(), ctx.getOrigin(), true);
      }
      return null;
   }

   private boolean shouldFlushL1(TxInvocationContext ctx) {
      return !ctx.getAffectedKeys().isEmpty();
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
