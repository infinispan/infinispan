package org.infinispan.interceptors.distribution;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.L1Manager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.impl.BaseRpcInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.transaction.TransactionMode;
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

   private static final Log log = LogFactory.getLog(L1NonTxInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private L1Manager l1Manager;
   private ClusteringDependentLogic cdl;
   private Configuration configuration;

   private boolean nonTransactional;

   @Inject
   public void init(L1Manager l1Manager, ClusteringDependentLogic cdl, Configuration configuration) {
      this.l1Manager = l1Manager;
      this.cdl = cdl;
      this.configuration = configuration;
   }

   @Start
   public void start() {
      nonTransactional = configuration.transaction().transactionMode() == TransactionMode.NON_TRANSACTIONAL;
   }

   @Override
   public BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command, true);
   }

   @Override
   public BasicInvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command, true);
   }

   @Override
   public BasicInvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command, false);
   }

   public BasicInvocationStage visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command, boolean assumeOriginKeptEntryInL1) throws Throwable {
      return invokeNext(ctx, command).thenApply((rCtx, rCommand, rv) -> {
         Object key;
         DataWriteCommand writeCommand = (DataWriteCommand) rCommand;
         if (shouldUpdateOnWriteCommand(writeCommand) && writeCommand.isSuccessful() &&
               cdl.localNodeIsOwner((key = writeCommand.getKey()))) {
            if (trace) {
               log.trace("Sending additional invalidation for requestors if necessary.");
            }
            // Send out a last attempt L1 invalidation in case if someone cached the L1
            // value after they already received an invalidation
            blockOnL1FutureIfNeeded(l1Manager
                  .flushCache(Collections.singleton(key), rCtx.getOrigin(), assumeOriginKeptEntryInL1));
         }
         return rv;
      });
   }

   @Override
   public BasicInvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return invokeNext(ctx, command).thenAccept((rCtx, rCommand, rv) -> {
         PutMapCommand putMapCommand = (PutMapCommand) rCommand;
         if (shouldUpdateOnWriteCommand(putMapCommand)) {
            Set<Object> keys = putMapCommand.getMap().keySet();
            Set<Object> toInvalidate = new HashSet<Object>(keys.size());
            for (Object k : keys) {
               if (cdl.localNodeIsOwner(k)) {
                  toInvalidate.add(k);
               }
            }
            if (!toInvalidate.isEmpty()) {
               if (trace) {
                  log.trace("Sending additional invalidation for requestors if necessary.");
               }
               blockOnL1FutureIfNeeded(l1Manager.flushCache(toInvalidate, rCtx.getOrigin(), true));
            }
         }
      });
   }

   private boolean shouldUpdateOnWriteCommand(WriteCommand command) {
      return nonTransactional && !command.hasFlag(Flag.CACHE_MODE_LOCAL);
   }

   @Override
   public BasicInvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return invokeNext(ctx, command).thenApply((rCtx, rCommand, rv) -> {
         if (((PrepareCommand) rCommand).isOnePhaseCommit()) {
            blockOnL1FutureIfNeededTx(handleLastChanceL1InvalidationOnCommit(((TxInvocationContext<?>) rCtx)));
         }
         return rv;
      });
   }

   @Override
   public BasicInvocationStage visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNext(ctx, command).thenApply((rCtx, rCommand, rv) -> {
         blockOnL1FutureIfNeededTx(handleLastChanceL1InvalidationOnCommit((TxInvocationContext<?>) rCtx));
         return rv;
      });
   }

   private Future<?> handleLastChanceL1InvalidationOnCommit(TxInvocationContext<?> ctx) {
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

   private void blockOnL1FutureIfNeededTx(Future<?> f) {
      if (configuration.transaction().syncCommitPhase()) {
         blockOnL1FutureIfNeeded(f);
      }
   }

   private void blockOnL1FutureIfNeeded(Future<?> f) {
      if (f != null) {
         try {
            f.get();
         } catch (InterruptedException e) {
            getLog().failedInvalidatingRemoteCache(e);
         } catch (ExecutionException e) {
            // Ignore SuspectExceptions - if the node has gone away then there is nothing to invalidate anyway.
            if (!(e.getCause() instanceof SuspectException)) {
               getLog().failedInvalidatingRemoteCache(e);
            }
         }
      }
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
