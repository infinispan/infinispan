package org.infinispan.interceptors.impl;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.util.logging.Log;

/**
 * Acts as a base for all RPC calls
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @since 9.0
 */
public abstract class BaseRpcInterceptor extends DDAsyncInterceptor {
   @Inject protected RpcManager rpcManager;
   @Inject protected ComponentRegistry componentRegistry;

   protected boolean defaultSynchronous;

   protected abstract Log getLog();

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
      return command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL);
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

   protected boolean shouldLoad(InvocationContext ctx, FlagAffectedCommand command, DistributionInfo info) {
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_REMOTE_LOOKUP))
         return false;

      VisitableCommand.LoadType loadType = command.loadType();
      switch (loadType) {
         case DONT_LOAD:
            return false;
         case OWNER:
            return info.isPrimary() || (info.isWriteOwner() && !ctx.isOriginLocal());
         case PRIMARY:
            return info.isPrimary();
         default:
            throw new IllegalStateException();
      }
   }
}
