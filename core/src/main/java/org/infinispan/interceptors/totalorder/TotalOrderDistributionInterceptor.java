package org.infinispan.interceptors.totalorder;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This interceptor handles distribution of entries across a cluster, as well as transparent lookup, when the total
 * order based protocol is enabled
 *
 * @author Pedro Ruivo
 */
public class TotalOrderDistributionInterceptor extends TxDistributionInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   public BasicInvocationStage visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (Configurations.isOnePhaseTotalOrderCommit(cacheConfiguration) || !ctx.hasModifications() ||
            !shouldTotalOrderRollbackBeInvokedRemotely(ctx)) {
         return invokeNext(ctx, command);
      }
      totalOrderTxRollback(ctx);
      return super.visitRollbackCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (Configurations.isOnePhaseTotalOrderCommit(cacheConfiguration) || !ctx.hasModifications()) {
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

      return totalOrderPrepare(ctx, command, recipients, isSyncCommitPhase() ? null : getSelfDeliverFilter());
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
