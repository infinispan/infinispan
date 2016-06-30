package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;

import java.util.concurrent.CompletableFuture;

/**
 * The default {@link Runnable} for the remote commands receives.
 * <p/>
 * It checks the command topology and ensures that the topology higher or equal is installed in this node.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class DefaultTopologyRunnable extends BaseBlockingRunnable {

   private final TopologyMode topologyMode;
   private final int commandTopologyId;

   public DefaultTopologyRunnable(BasePerCacheInboundInvocationHandler handler, CacheRpcCommand command, Reply reply,
                                  TopologyMode topologyMode, int commandTopologyId) {
      super(handler, command, reply);
      this.topologyMode = topologyMode;
      this.commandTopologyId = commandTopologyId;
   }

   @Override
   public boolean isReady() {
      switch (topologyMode) {
         case READY_TOPOLOGY:
            return handler.getStateTransferLock().topologyReceived(waitTopology());
         case READY_TX_DATA:
            return handler.getStateTransferLock().transactionDataReceived(waitTopology());
         default:
            return true;
      }
   }

   @Override
   protected InvocationStatus beforeInvoke() throws Exception {
      // Read commands are executed directly in OOB thread pool. When the many commands are received prior
      // to the first topology update (when the node is joining), these can wait for transaction data
      // (even in non-tx mode) and deplete OOB TP => the node stops being responsive event to ST.
      CompletableFuture<Void> future;
      switch (topologyMode) {
         case WAIT_TOPOLOGY:
            future = handler.getStateTransferLock().topologyFuture(waitTopology());
            if (future != null) {
               future.thenRun(this);
               return InvocationStatus.DEFERRED;
            }
            break;
         case WAIT_TX_DATA:
            future = handler.getStateTransferLock().transactionDataFuture(waitTopology());
            if (future != null) {
               future.thenRun(this);
               return InvocationStatus.DEFERRED;
            }
            break;
         default:
            break;
      }
      if (handler.isCommandSentBeforeFirstTopology(commandTopologyId)) {
         return InvocationStatus.CACHE_NOT_FOUND;
      }
      return super.beforeInvoke();
   }

   private int waitTopology() {
      // Always wait for the first topology (i.e. for the join to finish)
      return Math.max(commandTopologyId, 0);
   }

}
