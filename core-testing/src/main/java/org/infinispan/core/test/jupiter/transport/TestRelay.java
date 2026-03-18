package org.infinispan.core.test.jupiter.transport;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.LOCAL_PING;
import org.jgroups.protocols.MFC;
import org.jgroups.protocols.UFC;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.Protocol;

/**
 * Custom RELAY2 for testing: uses in-JVM bridge channels with {@link LOCAL_PING}
 * discovery. Sites are registered via the static {@link #registerTopology} method
 * before cache managers are created, and the relay configures itself from that
 * registry using the {@code bridge_name} property as the lookup key.
 *
 * @since 16.2
 */
public class TestRelay extends RELAY2 {

   static {
      ClassConfigurator.addProtocol((short) 1322, TestRelay.class);
   }

   private static final Map<String, List<String>> TOPOLOGY_REGISTRY = new ConcurrentHashMap<>();

   @Property(description = "Unique bridge cluster name, also used to look up the site topology")
   protected String bridge_name;

   /**
    * Registers a topology so that TestRelay instances can look it up during initialization.
    *
    * @param bridgeClusterName unique bridge cluster name
    * @param siteNames         all site names in the topology
    */
   public static void registerTopology(String bridgeClusterName, List<String> siteNames) {
      TOPOLOGY_REGISTRY.put(bridgeClusterName, List.copyOf(siteNames));
   }

   /**
    * Removes a previously registered topology.
    */
   public static void unregisterTopology(String bridgeClusterName) {
      TOPOLOGY_REGISTRY.remove(bridgeClusterName);
   }

   @Override
   public void configure() throws Exception {
      // Populate sites from the registry before super.configure() checks them
      List<String> siteNames = TOPOLOGY_REGISTRY.get(bridge_name);
      if (siteNames != null) {
         for (String siteName : siteNames) {
            RelayConfig.SiteConfig siteConfig = new RelayConfig.SiteConfig(siteName);
            siteConfig.addBridge(new RelayConfig.ProgrammaticBridgeConfig(
                  bridge_name, bridgeStack()));
            addSite(siteName, siteConfig);
         }
      }
      super.configure();
   }

   private static Protocol[] bridgeStack() {
      return new Protocol[]{
            new org.jgroups.protocols.TCP()
                  .setBindAddress(java.net.InetAddress.getLoopbackAddress())
                  .setBindPort(0),
            new LOCAL_PING(),
            new NAKACK2(),
            new UNICAST3(),
            new STABLE(),
            new GMS().setJoinTimeout(1000).printLocalAddress(false),
            new UFC(),
            new MFC()
      };
   }
}
