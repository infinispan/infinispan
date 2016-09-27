package org.infinispan.interceptors.impl;

import java.util.Collections;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This interceptor populates the {@link DldGlobalTransaction} with
 * appropriate information needed in order to accomplish deadlock detection. It MUST populate data before the
 * replication takes place.
 * <p/>
 * Note: for local caches, deadlock detection dos NOT work for aggregate operations (clear, putAll).
 *
 * @author Mircea.Markus@jboss.com
 * @since 9.0
 */
public class DeadlockDetectingInterceptor extends DDAsyncInterceptor {

   private static final Log log = LogFactory.getLog(DeadlockDetectingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   /**
    * Only does a sanity check.
    */
   @Start
   public void start() {
      if (!cacheConfiguration.deadlockDetection().enabled()) {
         throw new IllegalStateException("This interceptor should not be present in the chain as deadlock detection is not used!");
      }
   }

   @Override
   public BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      DldGlobalTransaction globalTransaction = (DldGlobalTransaction) ctx.getGlobalTransaction();
      if (ctx.isOriginLocal()) {
         globalTransaction.setRemoteLockIntention(command.getKeys());
         //in the case of DIST we need to propagate the list of keys. In all other situations in can be determined
         // based on the actual command
         CacheMode cacheMode = cacheConfiguration.clustering().cacheMode();
         if (cacheMode.isDistributed() || cacheMode.isReplicated()) {
            if (trace) log.tracef("Locks as seen at origin are: %s", ctx.getLockedKeys());
            ((DldGlobalTransaction) ctx.getGlobalTransaction()).setLocksHeldAtOrigin(ctx.getLockedKeys());
         }
      }
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      DldGlobalTransaction globalTransaction = (DldGlobalTransaction) ctx.getGlobalTransaction();
      if (ctx.isOriginLocal()) {
         globalTransaction.setRemoteLockIntention(command.getAffectedKeys());
      }
      return invokeNext(ctx, command).thenAccept((rCtx, rCommand, rv) -> {
         if (rCtx.isOriginLocal()) {
            globalTransaction.setRemoteLockIntention(Collections.emptySet());
         }
      });
   }
}
