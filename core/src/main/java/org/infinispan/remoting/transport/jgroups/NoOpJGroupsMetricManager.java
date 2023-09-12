package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.transport.Address;
import org.jgroups.JChannel;

/**
 * An empty implementation of {@link JGroupsMetricsManager}.
 * <p>
 * All methods are no-op except for {@link #trackRequest(Address)} which return an instance of
 * {@link NoOpRequestTracker}.
 *
 * @see NoOpRequestTracker
 */
public final class NoOpJGroupsMetricManager implements JGroupsMetricsManager {

   public static final JGroupsMetricsManager INSTANCE = new NoOpJGroupsMetricManager();

   private NoOpJGroupsMetricManager() {
   }

   @Override
   public RequestTracker trackRequest(Address destination) {
      return new NoOpRequestTracker(destination);
   }

   @Override
   public void recordMessageSent(Address destination, int bytesSent, boolean async) {
      //no-op
   }

   @Override
   public void onChannelConnected(JChannel channel, boolean isMainChannel) {
      //no-op
   }

   @Override
   public void onChannelDisconnected(JChannel channel) {
      //no-op
   }

}
