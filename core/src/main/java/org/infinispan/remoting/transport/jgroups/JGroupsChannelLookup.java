package org.infinispan.remoting.transport.jgroups;

import org.jgroups.Channel;

import java.util.Properties;

/**
 * A hook to pass in a JGroups channel.  Implementations need to provide a public no-arg constructor as instances are
 * created via reflection.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface JGroupsChannelLookup {
   /**
    * Retrieves a JGroups channel.  Passes in all of the properties used to configure the channel.
    * @param p properties
    * @return a JGroups channel
    */
   Channel getJGroupsChannel(Properties p);

   /**
    * @return true if the JGroupsTransport should start and connect the channel before using it; false if the transport
    * should assume that the channel is connected and started.
    */
   boolean shouldStartAndConnect();

   /**
    * @return true if the JGroupsTransport should stop and disconnect the channel once it is done with it; false if
    * the channel is to be left open/connected.
    */
   boolean shouldStopAndDisconnect();
}
