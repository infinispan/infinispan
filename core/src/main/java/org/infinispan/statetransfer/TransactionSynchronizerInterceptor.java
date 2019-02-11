package org.infinispan.statetransfer;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
 * otherwise we can end up in deadlocks when a command is forwarded in a loop to the same cache: e.g. A&rarr;B&rarr;C&rarr;A. This
 * scenario is possible when we have chained topology changes (see <a href="https://issues.jboss.org/browse/ISPN-2578">ISPN-2578</a>).
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class TransactionSynchronizerInterceptor extends BaseAsyncInterceptor {
   private static final Log log = LogFactory.getLog(TransactionSynchronizerInterceptor.class);

   @Override
   public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (ctx.isOriginLocal() || !(command instanceof TransactionBoundaryCommand)) {
         return invokeNext(ctx, command);
      }

      CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
      RemoteTransaction remoteTransaction = ((TxInvocationContext<RemoteTransaction>) ctx).getCacheTransaction();
      Object result = asyncInvokeNext(ctx, command, remoteTransaction.enterSynchronizationAsync(releaseFuture));
      return makeStage(result).andFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
               log.tracef("Completing tx command release future for %s", remoteTransaction);
               releaseFuture.complete(null);
            });
   }
}
