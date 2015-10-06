package org.infinispan.interceptors.sequential;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SelfDeliverFilter;
import org.infinispan.remoting.responses.TimeoutValidationResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Acts as a base for all RPC calls
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @since 8.1
 */
public abstract class BaseRpcInterceptor extends BaseSequentialInterceptor {
   private static final Log log = LogFactory.getLog(BaseRpcInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   protected RpcManager rpcManager;

   protected boolean defaultSynchronous;

   @Inject
   public void inject(RpcManager rpcManager) {
      this.rpcManager = rpcManager;
   }

   @Start
   public void init() {
      defaultSynchronous = cacheConfiguration.clustering().cacheMode().isSynchronous();
   }

   protected final boolean isSynchronous(FlagAffectedCommand command) {
      if (command.hasFlag(Flag.FORCE_SYNCHRONOUS))
         return true;
      else if (command.hasFlag(Flag.FORCE_ASYNCHRONOUS))
         return false;

      return defaultSynchronous;
   }

   protected final boolean isLocalModeForced(FlagAffectedCommand command) {
      if (command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (log.isTraceEnabled())
            log.trace("LOCAL mode forced on invocation.  Suppressing clustered events.");
         return true;
      }
      return false;
   }

   protected boolean shouldInvokeRemoteTxCommand(TxInvocationContext ctx) {
      if (!ctx.isOriginLocal()) {
         return false;
      }

      // Skip the remote invocation if this is a state transfer transaction
      LocalTransaction localTx = (LocalTransaction) ctx.getCacheTransaction();
      if (localTx.getStateTransferFlag() == Flag.PUT_FOR_STATE_TRANSFER) {
         return false;
      }

      // just testing for empty modifications isn't enough - the Lock API may acquire locks on keys but won't
      // register a Modification.  See ISPN-711.
      boolean shouldInvokeRemotely = ctx.hasModifications() || !localTx.getRemoteLocksAcquired().isEmpty() ||
            localTx.getTopologyId() != rpcManager.getTopologyId();

      if (trace) {
         log.tracef("Should invoke remotely? %b. hasModifications=%b, hasRemoteLocksAcquired=%b",
                    shouldInvokeRemotely, ctx.hasModifications(),
                    !localTx.getRemoteLocksAcquired().isEmpty());
      }

      return shouldInvokeRemotely;
   }

   protected static void transactionRemotelyPrepared(TxInvocationContext ctx) {
      if (ctx.isOriginLocal()) {
         ((LocalTransaction) ctx.getCacheTransaction()).markPrepareSent();
      }
   }

   protected static void totalOrderTxCommit(TxInvocationContext ctx) {
      if (ctx.isOriginLocal()) {
         ((LocalTransaction) ctx.getCacheTransaction()).markCommitOrRollbackSent();
      }
   }

   protected static void totalOrderTxRollback(TxInvocationContext ctx) {
      if (ctx.isOriginLocal()) {
         ((LocalTransaction) ctx.getCacheTransaction()).markCommitOrRollbackSent();
      }
   }

   protected static boolean shouldTotalOrderRollbackBeInvokedRemotely(TxInvocationContext ctx) {
      return ctx.isOriginLocal() && ((LocalTransaction) ctx.getCacheTransaction()).isPrepareSent() &&
            !((LocalTransaction) ctx.getCacheTransaction()).isCommitOrRollbackSent();
   }

   protected final CompletableFuture<Map<Address, Response>> totalOrderPrepare(Collection<Address> recipients,
                                                                               PrepareCommand prepareCommand,
                                                                               TimeoutValidationResponseFilter responseFilter) {
      Set<Address> realRecipients = null;
      if (recipients != null) {
         realRecipients = new HashSet<>(recipients);
         realRecipients.add(rpcManager.getAddress());
      }
      return internalTotalOrderPrepare(realRecipients, prepareCommand, responseFilter);
   }

   private CompletableFuture<Map<Address, Response>> internalTotalOrderPrepare(Collection<Address> recipients,
                                                                               PrepareCommand prepareCommand,
                                                                               TimeoutValidationResponseFilter responseFilter) {
      if (defaultSynchronous) {
         RpcOptionsBuilder builder =
               rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.TOTAL);
         if (responseFilter != null) {
            builder.responseFilter(responseFilter);
         }
         CompletableFuture<Map<Address, Response>> future =
               rpcManager.invokeRemotelyAsync(recipients, prepareCommand, builder.build());
         if (responseFilter != null) {
            return future.thenApply(responses -> {
               responseFilter.validate();
               return responses;
            });
         }
         return future;
      } else {
         RpcOptionsBuilder builder =
               rpcManager.getRpcOptionsBuilder(ResponseMode.ASYNCHRONOUS, DeliverOrder.TOTAL);
         return rpcManager.invokeRemotelyAsync(recipients, prepareCommand, builder.build());
      }
   }

   protected final boolean isSyncCommitPhase() {
      return cacheConfiguration.transaction().syncCommitPhase();
   }

   protected final TimeoutValidationResponseFilter getSelfDeliverFilter() {
      return new SelfDeliverFilter(rpcManager.getAddress());
   }
}
