package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.transport.Address;
import org.jgroups.JChannel;

/**
 * Stores and register metrics statistics related to JGroups, including the protocol metrics and per destination metrics
 * information (latency, bytes sent, etc.).
 */
public interface JGroupsMetricsManager {

   /**
    * Track the latency for a synchronous request.
    *
    * @param destination The destination address. Cannot be null.
    * @return A {@link RequestTracker} implementation with the send time already set.
    */
   RequestTracker trackRequest(Address destination);

   /**
    * Records a message sent to a {@code destination}.
    * <p>
    * Updates the bytes sent and, if it is an async message, the async counter.
    *
    * @param destination The destination address. Cannot be null.
    * @param bytesSent   The number of bytes sent in the message.
    * @param async       Set to {@code true} if the message is asynchronous.
    */
   void recordMessageSent(Address destination, int bytesSent, boolean async);

   /**
    * Registers metrics for a {@link JChannel}.
    *
    * @param channel       The {@link JChannel} instance.
    * @param isMainChannel Set to {@code true} if this is the main channel (the cluster channel, not cross-site).
    */
   void onChannelConnected(JChannel channel, boolean isMainChannel);

   /**
    * Unregisters metrics for a {@link JChannel}.
    *
    * @param channel The {@link JChannel} instance.
    */
   void onChannelDisconnected(JChannel channel);

}
