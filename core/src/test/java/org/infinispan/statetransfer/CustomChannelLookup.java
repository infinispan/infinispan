package org.infinispan.statetransfer;

import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.TransportConfigurationBuilder;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelLookup;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.JChannel;

/**
 * JGroupsChannelLookup implementation that returns an existing channel.
 *
 * @author Dan Berindei
 * @since 8.2
 */
public class CustomChannelLookup implements JGroupsChannelLookup {
   private static final Map<String, JChannel> channelMap = CollectionFactory.makeConcurrentMap();
   private boolean connect;

   public static void registerChannel(GlobalConfigurationBuilder gcb, JChannel channel, String nodeName,
         boolean connect) {
      TransportConfigurationBuilder tcb = gcb.transport();
      tcb.defaultTransport();
      tcb.addProperty(JGroupsTransport.CHANNEL_LOOKUP, CustomChannelLookup.class.getName());
      tcb.addProperty("customNodeName", nodeName);
      tcb.addProperty("customConnect", Boolean.toString(connect));
      channelMap.put(nodeName, channel);
   }

   @Override
   public JChannel getJGroupsChannel(Properties p) {
      String nodeName = p.getProperty("customNodeName");
      connect = Boolean.valueOf(p.getProperty("customConnect"));
      return channelMap.remove(nodeName);
   }

   @Override
   public boolean shouldConnect() {
      return connect;
   }

   @Override
   public boolean shouldDisconnect() {
      return true;
   }

   @Override
   public boolean shouldClose() {
      return true;
   }
}
