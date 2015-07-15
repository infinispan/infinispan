package org.infinispan.remoting.inboundhandler;

/**
 * It checks or waits until the required topology is installed.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public enum TopologyMode {
   WAIT_TOPOLOGY,
   READY_TOPOLOGY,
   WAIT_TX_DATA,
   READY_TX_DATA;

   public static TopologyMode create(boolean onExecutor, boolean txData) {
      if (onExecutor && txData) {
         return TopologyMode.READY_TX_DATA;
      } else if (onExecutor) {
         return TopologyMode.READY_TOPOLOGY;
      } else if (txData) {
         return TopologyMode.WAIT_TX_DATA;
      } else {
         return TopologyMode.WAIT_TOPOLOGY;
      }
   }
}
