package org.infinispan.remoting.inboundhandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.util.concurrent.CompletionStages;

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
   protected final int commandTopologyId;

   public DefaultTopologyRunnable(BasePerCacheInboundInvocationHandler handler, CacheRpcCommand command, Reply reply,
                                  TopologyMode topologyMode, int commandTopologyId, boolean sync) {
      super(handler, command, reply, sync);
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
   protected CompletionStage<CacheNotFoundResponse> beforeInvoke() {
      CompletionStage<Void> stage = null;
      switch (topologyMode) {
         case WAIT_TOPOLOGY:
            stage = handler.getStateTransferLock().topologyFuture(waitTopology());
            break;
         case WAIT_TX_DATA:
            stage = handler.getStateTransferLock().transactionDataFuture(waitTopology());
            break;
         default:
            break;
      }
      if (stage != null && !CompletionStages.isCompletedSuccessfully(stage)) {
         return stage.thenApply(nil -> handler.isCommandSentBeforeFirstTopology(commandTopologyId) ?
               CacheNotFoundResponse.INSTANCE :
               null);
      } else {
         return handler.isCommandSentBeforeFirstTopology(commandTopologyId) ?
               CompletableFuture.completedFuture(CacheNotFoundResponse.INSTANCE) :
               null;
      }
   }

   private int waitTopology() {
      // Always wait for the first topology (i.e. for the join to finish)
      return Math.max(commandTopologyId, 0);
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("DefaultTopologyRunnable{");
      sb.append("topologyMode=").append(topologyMode);
      sb.append(", commandTopologyId=").append(commandTopologyId);
      sb.append(", command=").append(command);
      sb.append(", sync=").append(sync);
      sb.append('}');
      return sb.toString();
   }
}
