package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;

import java.util.concurrent.TimeUnit;

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
   protected Response beforeInvoke() throws Exception {
      switch (topologyMode) {
         case WAIT_TOPOLOGY:
            handler.getStateTransferLock().waitForTopology(waitTopology(), 1, TimeUnit.DAYS);
            break;
         case WAIT_TX_DATA:
            handler.getStateTransferLock().waitForTransactionData(waitTopology(), 1, TimeUnit.DAYS);
            break;
         default:
            break;
      }
      if (handler.isCommandSentBeforeFirstTopology(commandTopologyId)) {
         return CacheNotFoundResponse.INSTANCE;
      }
      return super.beforeInvoke();
   }

   private int waitTopology() {
      // Always wait for the first topology (i.e. for the join to finish)
      return Math.max(commandTopologyId, 0);
   }

}
