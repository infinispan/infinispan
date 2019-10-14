/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.access;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.Ownership;
import org.infinispan.hibernate.cache.commons.util.CompletableFunction;
import org.infinispan.interceptors.InvocationFinallyFunction;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * With regular {@link org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor},
 * async replication does not work in combination with synchronous replication: sync replication
 * relies on locking to order writes on backup while async replication relies on FIFO-ordering
 * from primary to backup. If these two combine, there's a possibility that on backup two modifications
 * modifications will proceed concurrently.
 * Similar issue threatens consistency when the command has {@link org.infinispan.context.Flag#CACHE_MODE_LOCAL}
 * - these commands don't acquire locks either.
 *
 * Therefore, this interceptor locks the entry all the time. {@link UnorderedDistributionInterceptor} does not forward
 * the message from non-origin to any other node, and the distribution interceptor won't block on RPC but will return
 * {@link CompletableFuture} and we'll wait for it here.
 */
public class LockingInterceptor extends NonTransactionalLockingInterceptor {
	private static final Log log = LogFactory.getLog(LockingInterceptor.class);
	private static final boolean trace = log.isTraceEnabled();

   protected final InvocationFinallyFunction<DataWriteCommand> unlockAllReturnCheckCompletableFutureHandler = (rCtx, rCommand, rv, throwable) -> {
      lockManager.unlockAll(rCtx);
      if (throwable != null)
         throw throwable;

      if (rv instanceof CompletableFuture) {
         // See CompletableFunction javadoc for explanation
         if (rCommand instanceof ReadWriteKeyCommand) {
            Function function = ((ReadWriteKeyCommand) rCommand).getFunction();
            if (function instanceof CompletableFunction) {
               ((CompletableFunction) function).markComplete();
            }
         }
         // Similar to CompletableFunction above, signals that the command has been applied for non-functional commands
         rCommand.setFlagsBitSet(rCommand.getFlagsBitSet() & ~FlagBitSets.FORCE_WRITE_LOCK);

         // The future is produced in UnorderedDistributionInterceptor.
         // We need the EWI to commit the entry & unlock before the remote call completes
         // but here we wait for the other nodes, without blocking concurrent updates.
         return asyncValue((CompletableFuture) rv);
      } else {
         return rv;
      }
   };
   protected final InvocationFinallyFunction<DataWriteCommand> invokeNextAndUnlock = (rCtx, dataWriteCommand, rv, throwable) -> {
      if (throwable != null) {
         lockManager.unlockAll(rCtx);
         if (throwable instanceof TimeoutException && dataWriteCommand.hasAnyFlag(FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT)) {
            dataWriteCommand.fail();
            return null;
         }
         throw throwable;
      } else {
         return invokeNextAndHandle(rCtx, dataWriteCommand, unlockAllReturnCheckCompletableFutureHandler);
      }
   };

   @Override
   protected Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) {
      try {
         if (trace) {
            Ownership ownership = cdl.getCacheTopology().getDistribution(command.getKey()).writeOwnership();
            log.tracef( "Am I owner for key=%s ? %s", command.getKey(), ownership);
         }

         if (ctx.getLockOwner() == null) {
            ctx.setLockOwner( command.getCommandInvocationId() );
         }

         InvocationStage lockStage = lockAndRecord(ctx, command, command.getKey(), getLockTimeoutMillis(command));
         return lockStage.andHandle(ctx, command, invokeNextAndUnlock);
      }
      catch (Throwable t) {
         lockManager.unlockAll(ctx);
         throw t;
      }
   }

}
