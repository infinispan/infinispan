package org.infinispan.interceptors.totalorder;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.distribution.VersionedDistributionInterceptor;
import org.infinispan.remoting.responses.KeysValidateFilter;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This interceptor is used in total order in distributed mode when the write skew check is enabled. After sending the
 * prepare through TOA (Total Order Anycast), it blocks the execution thread until the transaction outcome is known
 * (i.e., the write skew check passes in all keys owners)
 *
 * @author Pedro Ruivo
 */
public class TotalOrderVersionedDistributionInterceptor extends VersionedDistributionInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderVersionedDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private boolean onePhaseTotalOrderCommit;

   @Override
   public void start() {
      super.start();
      onePhaseTotalOrderCommit = Configurations.isOnePhaseTotalOrderCommit(cacheConfiguration);
   }

   @Override
   public BasicInvocationStage visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (onePhaseTotalOrderCommit || !ctx.hasModifications() ||
            !shouldTotalOrderRollbackBeInvokedRemotely(ctx)) {
         return invokeNext(ctx, command);
      }
      totalOrderTxRollback(ctx);
      return super.visitRollbackCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (onePhaseTotalOrderCommit || !ctx.hasModifications()) {
         return invokeNext(ctx, command);
      }
      totalOrderTxCommit(ctx);
      return super.visitCommitCommand(ctx, command);
   }

   @Override
   protected CompletableFuture<Object> prepareOnAffectedNodes(TxInvocationContext<?> ctx, PrepareCommand command, Collection<Address> recipients) {
      if (trace) {
         log.tracef("Total Order Anycast transaction %s with Total Order", command.getGlobalTransaction().globalId());
      }

      if (!ctx.hasModifications()) {
         return CompletableFutures.completedNull();
      }

      if (!ctx.isOriginLocal()) {
         throw new IllegalStateException("Expected a local context while TO-Anycast prepare command");
      }

      if (!(command instanceof VersionedPrepareCommand)) {
         throw new IllegalStateException("Expected a Versioned Prepare Command in version aware component");
      }

      KeysValidateFilter responseFilter = ctx.getCacheTransaction().hasModification(ClearCommand.class) || isSyncCommitPhase() ?
            null : new KeysValidateFilter(rpcManager.getAddress(), ctx.getAffectedKeys());
      return totalOrderPrepare(ctx, command, recipients, responseFilter);
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
