package org.infinispan.interceptors.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
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
   protected final boolean trace = getLog().isTraceEnabled();

   protected RpcManager rpcManager;
   protected ComponentRegistry componentRegistry;

   protected boolean defaultSynchronous;

   protected abstract Log getLog();

   @Inject
   public void inject(RpcManager rpcManager, ComponentRegistry componentRegistry) {
      this.rpcManager = rpcManager;
      this.componentRegistry = componentRegistry;
   }

   @Start
   public void init() {
      defaultSynchronous = cacheConfiguration.clustering().cacheMode().isSynchronous();
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

   protected CompletionStage<Object> totalOrderPrepare(TxInvocationContext<?> ctx, PrepareCommand command,
                                                       Collection<Address> recipients) {
      try {
         CompletionStage<Map<Address, Response>> remoteInvocation;
         if (recipients != null) {
            Set<Address> realRecipients = new HashSet<>(recipients);
            realRecipients.add(rpcManager.getAddress());
            remoteInvocation = rpcManager.invokeCommand(realRecipients, command,
                                                        MapResponseCollector.ignoreLeavers(realRecipients.size()),
                                                        rpcManager.getTotalSyncRpcOptions());
         } else {
            remoteInvocation = rpcManager.invokeCommandOnAll(command,
                                                             MapResponseCollector.ignoreLeavers(),
                                                             rpcManager.getTotalSyncRpcOptions());
         }
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

}
