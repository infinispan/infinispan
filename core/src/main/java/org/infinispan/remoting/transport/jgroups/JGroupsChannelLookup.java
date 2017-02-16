package org.infinispan.remoting.transport.jgroups;

import java.util.Properties;

import org.jgroups.JChannel;

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
   JChannel getJGroupsChannel(Properties p);

   /**
    * @return true if the JGroupsTransport should connect the channel before using it; false if the transport
    * should assume that the channel is connected.
    */
   boolean shouldConnect();

   /**
    * @return true if the JGroupsTransport should disconnect the channel once it is done with it; false if
    * the channel is to be left connected.
    */
   boolean shouldDisconnect();

   /**
    * @return true if the JGroupsTransport should close the channel once it is done with it; false if
    * the channel is to be left open.
    */
   boolean shouldClose();
}
