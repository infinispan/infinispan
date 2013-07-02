package org.infinispan.interceptors.base;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.responses.IgnoreExtraResponsesValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SelfDeliverFilter;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.transaction.LocalTransaction;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Acts as a base for all RPC calls
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class BaseRpcInterceptor extends CommandInterceptor {

   protected RpcManager rpcManager;
   private StateConsumer stateConsumer;

   protected boolean defaultSynchronous;

   @Inject
   public void inject(RpcManager rpcManager, StateConsumer stateConsumer) {
      this.rpcManager = rpcManager;
      this.stateConsumer = stateConsumer;
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
         if (getLog().isTraceEnabled()) getLog().trace("LOCAL mode forced on invocation.  Suppressing clustered events.");
         return true;
      }
      return false;
   }

   protected boolean shouldInvokeRemoteTxCommand(TxInvocationContext ctx) {
      if (!ctx.isOriginLocal()) {
         return false;
      }

      // just testing for empty modifications isn't enough - the Lock API may acquire locks on keys but won't
      // register a Modification.  See ISPN-711.
      LocalTxInvocationContext localCtx = (LocalTxInvocationContext) ctx;
      boolean shouldInvokeRemotely = ctx.hasModifications() || !localCtx.getRemoteLocksAcquired().isEmpty() ||
         localCtx.getCacheTransaction().getTopologyId() != rpcManager.getTopologyId();

      if (getLog().isTraceEnabled()) {
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

   protected final Map<Address, Response> totalOrderAnycastPrepare(Collection<Address> recipients,
                                                                   PrepareCommand prepareCommand,
                                                                   ResponseFilter responseFilter) {
      Set<Address> realRecipients = new HashSet<Address>(recipients);
      realRecipients.add(rpcManager.getAddress());
      return internalTotalOrderPrepare(realRecipients, prepareCommand, responseFilter);
   }

   protected final Map<Address, Response> totalOrderBroadcastPrepare(PrepareCommand prepareCommand, ResponseFilter responseFilter) {
      return internalTotalOrderPrepare(null, prepareCommand, responseFilter);
   }

   private Map<Address, Response> internalTotalOrderPrepare(Collection<Address> recipients, PrepareCommand prepareCommand,
                                                            ResponseFilter responseFilter) {
      if (defaultSynchronous) {
         RpcOptionsBuilder builder = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, false);
         if (responseFilter != null) {
            builder.responseFilter(responseFilter);
         } else {
            builder.responseFilter(getIgnoreExtraResponseFilter(recipients));
         }
         builder.totalOrder(true);
         return rpcManager.invokeRemotely(recipients, prepareCommand, builder.build());
      } else {
         RpcOptionsBuilder builder = rpcManager.getRpcOptionsBuilder(ResponseMode.getAsyncResponseMode(cacheConfiguration),
                                                                     false);
         builder.totalOrder(true);
         return rpcManager.invokeRemotely(recipients, prepareCommand, builder.build());
      }
   }

   protected final boolean isSyncCommitPhase() {
      return cacheConfiguration.transaction().syncCommitPhase();
   }

   protected final ResponseFilter getSelfDeliverFilter() {
      return new SelfDeliverFilter(rpcManager.getAddress());
   }

   protected final ResponseFilter getIgnoreExtraResponseFilter(Collection<Address> recipients) {
      Set<Address> liveMembers = new HashSet<Address>(stateConsumer.getCacheTopology().getMembers());
      if (recipients != null) {
         liveMembers.retainAll(recipients);
      }
      return new IgnoreExtraResponsesValidityFilter(liveMembers, rpcManager.getAddress(), false);
   }
}
