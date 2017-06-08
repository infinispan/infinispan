package org.infinispan.interceptors.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;

/**
 * Acts as a base for all RPC calls
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @since 9.0
 */
public abstract class BaseRpcInterceptor extends DDAsyncInterceptor {
   private final static ResponseFilter SUCCESSFUL_OR_EXCEPTIONAL = new ResponseFilter() {
      @Override
      public boolean isAcceptable(Response response, Address sender) {
         return response.isSuccessful() || response instanceof ExceptionResponse;
      }

      @Override
      public boolean needMoreResponses() {
         return true;
      }
   };

   protected final boolean trace = getLog().isTraceEnabled();

   protected RpcManager rpcManager;

   protected boolean defaultSynchronous;
   protected volatile RpcOptions singleTargetStaggeredOptions;
   protected volatile RpcOptions multiTargetStaggeredOptions;
   protected volatile RpcOptions defaultSyncOptions;
   protected volatile RpcOptions syncIgnoreLeavers;
   protected RpcOptions defaultAsyncOptions;

   protected abstract Log getLog();

   @Inject
   public void inject(RpcManager rpcManager) {
      this.rpcManager = rpcManager;
   }

   @Start
   public void init() {
      defaultSynchronous = cacheConfiguration.clustering().cacheMode().isSynchronous();
      cacheConfiguration.clustering().attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT)
            .addListener(((a, o) -> initRpcOptions()));
      initRpcOptions();
      // async options don't depend on the remote timeout
      defaultAsyncOptions = rpcManager.getDefaultRpcOptions(false);
   }

   private void initRpcOptions() {
      singleTargetStaggeredOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.PER_SENDER).build();
      multiTargetStaggeredOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.WAIT_FOR_VALID_RESPONSE, DeliverOrder.PER_SENDER)
            .responseFilter(SUCCESSFUL_OR_EXCEPTIONAL).build();
      defaultSyncOptions = rpcManager.getDefaultRpcOptions(true);
      syncIgnoreLeavers = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build();
   }

   protected RpcOptions getStaggeredOptions(int numTargets) {
      // TODO: handle better when dropping MessageDispatcher-based RPC
      // This is somwhat a hack to the way staggered reads are implemented: what we intend is to keep
      // the sender waiting until it receives successful response. If it did not receive any successful
      // response, just return those unsuccessful ones.
      // Staggered gets use the filter but add non-accepted responses to Responses anyway. JGroupsTransport
      // later won't filter those unaccepted values and we'll get the unsuccesful ones, too.
      // When there's only single target, the processing uses GroupRequest and that wouldn't return
      // filtered values add all, therefore we'll omit the filter in there.
      // Regrettably this cannot be handled properly by current filtering options.
      return numTargets == 1 ? singleTargetStaggeredOptions : multiTargetStaggeredOptions;
   }

   protected final boolean isSynchronous(FlagAffectedCommand command) {
      if (command.hasAnyFlag(FlagBitSets.FORCE_SYNCHRONOUS))
         return true;
      else if (command.hasAnyFlag(FlagBitSets.FORCE_ASYNCHRONOUS))
         return false;

      return defaultSynchronous;
   }

   protected final boolean isLocalModeForced(FlagAffectedCommand command) {
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
         if (trace) getLog().trace("LOCAL mode forced on invocation.  Suppressing clustered events.");
         return true;
      }
      return false;
   }

   protected boolean shouldInvokeRemoteTxCommand(TxInvocationContext ctx) {
      if (!ctx.isOriginLocal()) {
         return false;
      }

      // Skip the remote invocation if this is a state transfer transaction
      LocalTxInvocationContext localCtx = (LocalTxInvocationContext) ctx;
      if (localCtx.getCacheTransaction().getStateTransferFlag() == Flag.PUT_FOR_STATE_TRANSFER) {
         return false;
      }

      // just testing for empty modifications isn't enough - the Lock API may acquire locks on keys but won't
      // register a Modification.  See ISPN-711.
      boolean shouldInvokeRemotely = ctx.hasModifications() || !localCtx.getRemoteLocksAcquired().isEmpty() ||
         localCtx.getCacheTransaction().getTopologyId() != rpcManager.getTopologyId();

      if (trace) {
         getLog().tracef("Should invoke remotely? %b. hasModifications=%b, hasRemoteLocksAcquired=%b",
               shouldInvokeRemotely, ctx.hasModifications(), !localCtx.getRemoteLocksAcquired().isEmpty());
      }

      return shouldInvokeRemotely;
   }

   protected static void transactionRemotelyPrepared(TxInvocationContext ctx) {
      if (ctx.isOriginLocal()) {
         ((LocalTransaction)ctx.getCacheTransaction()).markPrepareSent();
      }
   }

   protected static void totalOrderTxCommit(TxInvocationContext ctx) {
      if (ctx.isOriginLocal()) {
         ((LocalTransaction)ctx.getCacheTransaction()).markCommitOrRollbackSent();
      }
   }

   protected static void totalOrderTxRollback(TxInvocationContext ctx) {
      if (ctx.isOriginLocal()) {
         ((LocalTransaction)ctx.getCacheTransaction()).markCommitOrRollbackSent();
      }
   }

   protected static boolean shouldTotalOrderRollbackBeInvokedRemotely(TxInvocationContext ctx) {
      return ctx.isOriginLocal() && ((LocalTransaction)ctx.getCacheTransaction()).isPrepareSent()
            && !((LocalTransaction)ctx.getCacheTransaction()).isCommitOrRollbackSent();
   }

   protected CompletableFuture<Object> totalOrderPrepare(TxInvocationContext<?> ctx, PrepareCommand command,
         Collection<Address> recipients) {
      try {
         Set<Address> realRecipients = null;
         if (recipients != null) {
            realRecipients = new HashSet<>(recipients);
            realRecipients.add(rpcManager.getAddress());
         }
         CompletableFuture<Map<Address, Response>> remoteInvocation =
               internalTotalOrderPrepare(realRecipients, command);
         return remoteInvocation.handle((responses, t) -> {
            transactionRemotelyPrepared(ctx);
            CompletableFutures.rethrowException(t);

            return null;
         });
      } catch (Throwable t) {
         transactionRemotelyPrepared(ctx);
         throw t;
      }
   }

   private CompletableFuture<Map<Address, Response>> internalTotalOrderPrepare(Collection<Address> recipients, PrepareCommand prepareCommand) {
      RpcOptionsBuilder builder = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.TOTAL);
      return rpcManager.invokeRemotelyAsync(recipients, prepareCommand, builder.build());
   }
}
