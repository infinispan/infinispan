package org.infinispan.statetransfer;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.xa.CacheTransaction;

/**
 * With the Non-Blocking State Transfer (NBST) in place it is possible for a transactional command to be forwarded
 * multiple times, concurrently to the same node. This interceptor makes sure that for any given transaction, the
 * interceptor chain, post {@link StateTransferInterceptor}, would only allows a single thread to amend a transaction.
 * </p>
 * E.g. of when this situation might occur:
 * <ul>
 * <li>1) Node A broadcasts PrepareCommand to nodes B, C </li>
 * <li>2) Node A leaves cluster, causing new topology to be installed </li>
 * <li>3) The command arrives to B and C, with lower topology than the current one</li>
 * <li>4) Both B and C forward the command to node D</li>
 * <li>5) D executes the two commands in parallel and finds out that A has left, therefore executing RollbackCommand></li>
 * </ul>
 * <p/>
 * This interceptor must placed after the logic that handles command forwarding ({@link StateTransferInterceptor}),
 * otherwise we can end up in deadlocks when a command is forwarded in a loop to the same cache: e.g. A->B->C->A. This
 * scenario is possible when we have chained topology changes (see <a href="https://issues.jboss.org/browse/ISPN-2578">ISPN-2578</a>).
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class TransactionSynchronizerInterceptor extends CommandInterceptor {

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         CacheTransaction cacheTransaction = ctx.getCacheTransaction();
         synchronized (cacheTransaction) {
            return super.visitPrepareCommand(ctx, command);
         }
      } else {
         return super.visitPrepareCommand(ctx, command);
      }
   }

   @Override
   public final Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         CacheTransaction cacheTransaction = ctx.getCacheTransaction();
         synchronized (cacheTransaction) {
            return super.visitCommitCommand(ctx, command);
         }
      } else {
         return super.visitCommitCommand(ctx, command);
      }
   }

   @Override
   public final Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         CacheTransaction cacheTransaction = ctx.getCacheTransaction();
         synchronized (cacheTransaction) {
            return super.visitRollbackCommand(ctx, command);
         }
      } else {
         return super.visitRollbackCommand(ctx, command);
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         CacheTransaction cacheTransaction = ctx.getCacheTransaction();
         synchronized (cacheTransaction) {
            return super.visitLockControlCommand(ctx, command);
         }
      } else {
         return super.visitLockControlCommand(ctx, command);
      }
   }
}
