package org.infinispan.remoting.transport.jgroups;

import org.jgroups.Channel;

/**
 * A hook to pass in a JGroups channel.  Implementations need to provide a public no-arg constructor as instances are
 * created via reflection.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface JGroupsChannelLookup {
   Channel getJGroupsChannel();

   boolean shouldStartAndConnect();

   boolean shouldStopAndDisconnect();
}
