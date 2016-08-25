package org.infinispan.test.fwk;

import static org.infinispan.commons.util.Immutables.immutableMapCopy;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.FD;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.FD_ALL;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.FD_ALL2;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.FD_SOCK;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.MERGE3;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.RELAY2;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.TCP;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.TCP_NIO2;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.TEST_PING;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.UDP;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.VERIFY_SUSPECT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.util.LegacyKeySupportSystemProperties;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.conf.XmlConfigurator;

/**
 * This class owns the logic of associating network resources(i.e. ports) with threads, in order to make sure that there
 * won't be any clashes between multiple clusters running in parallel on same host. Used for parallel test suite.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
public class JGroupsConfigBuilder {

   public static final String JGROUPS_STACK;
   // Load the XML just once
   private static final ProtocolStackConfigurator tcpConfigurator = loadTcp();
   private static final ProtocolStackConfigurator udpConfigurator = loadUdp();

   public static final int TCP_PORT_RANGE_PER_THREAD = 100;
   private static final ThreadLocal<Integer> threadTcpStartPort = new ThreadLocal<Integer>() {
      private final AtomicInteger uniqueAddr = new AtomicInteger(7900);

      @Override
      protected Integer initialValue() {
         return uniqueAddr.getAndAdd(TCP_PORT_RANGE_PER_THREAD);
      }
   };

   /**
    * Holds unique mcast_addr for each thread used for JGroups channel construction.
    */
   private static final ThreadLocal<String> threadMcastIP = new ThreadLocal<String>() {
      private final AtomicInteger uniqueAddr = new AtomicInteger(11);

      @Override
      protected String initialValue() {
         return "228.10.10." + uniqueAddr.getAndIncrement();
      }
   };

   /**
    * Holds unique mcast_port for each thread used for JGroups channel construction.
    */
   private static final ThreadLocal<Integer> threadMcastPort = new ThreadLocal<Integer>() {
      private final AtomicInteger uniquePort = new AtomicInteger(45589);

      @Override
      protected Integer initialValue() {
         return uniquePort.getAndIncrement();
      }
   };

   static {
      JGROUPS_STACK = LegacyKeySupportSystemProperties.getProperty("infinispan.test.jgroups.protocol", "protocol.stack", "tcp");
      System.out.println("Transport protocol stack used = " + JGROUPS_STACK);
   }

   public static String getJGroupsConfig(String fullTestName, TransportFlags flags) {
      if (JGROUPS_STACK.equalsIgnoreCase("tcp")) return getTcpConfig(fullTestName, flags);
      if (JGROUPS_STACK.equalsIgnoreCase("udp")) return getUdpConfig(fullTestName, flags);
      throw new IllegalStateException("Unknown protocol stack : " + JGROUPS_STACK);
   }

   public static String getTcpConfig(String fullTestName, TransportFlags flags) {
      // With the XML already parsed, make a safe copy of the
      // protocol stack configurator and use that accordingly.
      JGroupsProtocolCfg jgroupsCfg =
            getJGroupsProtocolCfg(tcpConfigurator.getProtocolStack());

      if (!flags.withFD())
         removeFailureDetection(jgroupsCfg);

      if (!flags.isRelayRequired()) {
         removeRelay2(jgroupsCfg);
      } else {
         ProtocolConfiguration protocol = jgroupsCfg.getProtocol(RELAY2);
         protocol.getProperties().put("site", flags.siteName());
         if (flags.relayConfig() != null) //if not specified, use default
            protocol.getProperties().put("config", flags.relayConfig());
      }

      if (!flags.withMerge())
         removeMerge(jgroupsCfg);

      if (jgroupsCfg.containsProtocol(TEST_PING)) {
         replaceTcpStartPort(jgroupsCfg, flags);
         if (fullTestName == null)
            return jgroupsCfg.toString(); // IDE run of test
         else
            return getTestPingDiscovery(fullTestName, jgroupsCfg); // Cmd line test run
      } else {
         return replaceMCastAddressAndPort(jgroupsCfg);
      }
   }

   private static void removeMerge(JGroupsProtocolCfg jgroupsCfg) {
      jgroupsCfg.removeProtocol(MERGE3);
   }

   public static String getUdpConfig(String fullTestName, TransportFlags flags) {
      JGroupsProtocolCfg jgroupsCfg = getJGroupsProtocolCfg(udpConfigurator.getProtocolStack());

      if (!flags.withFD())
         removeFailureDetection(jgroupsCfg);

      if (!flags.withMerge())
         removeMerge(jgroupsCfg);

      if (!flags.isRelayRequired()) {
         removeRelay2(jgroupsCfg);
      }

      if (jgroupsCfg.containsProtocol(TEST_PING)) {
         if (fullTestName != null)
            return getTestPingDiscovery(fullTestName, jgroupsCfg); // Cmd line test run
      }

      return replaceMCastAddressAndPort(jgroupsCfg);
   }

   /**
    * Remove all failure detection related
    * protocols from the given JGroups stack.
    */
   private static void removeFailureDetection(JGroupsProtocolCfg jgroupsCfg) {
      jgroupsCfg.removeProtocol(FD).removeProtocol(FD_SOCK).removeProtocol(FD_ALL).removeProtocol(FD_ALL2)
            .removeProtocol(VERIFY_SUSPECT);
   }

   private static void removeRelay2(JGroupsProtocolCfg jgroupsCfg) {
      jgroupsCfg.removeProtocol(RELAY2);
   }

   private static String getTestPingDiscovery(String fullTestName, JGroupsProtocolCfg jgroupsCfg) {
      ProtocolType type = TEST_PING;
      Map<String, String> props = jgroupsCfg.getProtocol(type).getProperties();
      props.put("testName", fullTestName);
      return replaceProperties(jgroupsCfg, props, type);
   }

   private static String replaceMCastAddressAndPort(JGroupsProtocolCfg jgroupsCfg) {
      ProtocolConfiguration udp = jgroupsCfg.getProtocol(UDP);
      if (udp == null) return jgroupsCfg.toString();

      Map<String, String> props = udp.getProperties();
      props.put("mcast_addr", threadMcastIP.get());
      props.put("mcast_port", threadMcastPort.get().toString());
      return replaceProperties(jgroupsCfg, props, UDP);
   }

   private static String replaceTcpStartPort(JGroupsProtocolCfg jgroupsCfg, TransportFlags transportFlags) {
      ProtocolType transportProtocol = jgroupsCfg.containsProtocol(TCP_NIO2) ? TCP_NIO2 : TCP;
      Map<String, String> props = jgroupsCfg.getProtocol(transportProtocol).getProperties();
      Integer startPort = threadTcpStartPort.get();
      int portRange = TCP_PORT_RANGE_PER_THREAD;
      if (transportFlags.isPortRangeSpecified()) {
         portRange = 10;
         int maxIndex = TCP_PORT_RANGE_PER_THREAD / portRange - 1;
         if (transportFlags.portRange() > maxIndex) {
            throw new IllegalStateException("Currently we only support " + (maxIndex + 1) + " ranges/sites!");
         }
         startPort += transportFlags.portRange() * portRange;
      }
      props.put("bind_port", startPort.toString());
      // In JGroups, the port_range is inclusive
      props.put("port_range", String.valueOf(portRange - 1));
      return replaceProperties(jgroupsCfg, props, transportProtocol);
   }

   private static String replaceProperties(
         JGroupsProtocolCfg cfg, Map<String, String> newProps, ProtocolType type) {
      ProtocolConfiguration protocol = cfg.getProtocol(type);
      ProtocolConfiguration newProtocol =
            new ProtocolConfiguration(protocol.getProtocolName(), newProps);
      cfg.replaceProtocol(type, newProtocol);
      return cfg.toString();
   }

   private static ProtocolStackConfigurator loadTcp() {
      try {
         return ConfiguratorFactory.getStackConfigurator("stacks/tcp.xml");
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private static ProtocolStackConfigurator loadUdp() {
      try {
         return ConfiguratorFactory.getStackConfigurator("stacks/udp.xml");
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private static JGroupsProtocolCfg getJGroupsProtocolCfg(List<ProtocolConfiguration> baseStack) {
      JGroupsXmxlConfigurator configurator = new JGroupsXmxlConfigurator(baseStack);
      List<ProtocolConfiguration> protoStack = configurator.getProtocolStack();
      Map<ProtocolType, ProtocolConfiguration> protoMap = new HashMap<ProtocolType, ProtocolConfiguration>(protoStack.size());
      for (ProtocolConfiguration cfg : protoStack)
         protoMap.put(getProtocolType(cfg.getProtocolName()), cfg);

      return new JGroupsProtocolCfg(protoMap, configurator);
   }

   private static ProtocolType getProtocolType(String name) {
      int dotIndex = name.lastIndexOf(".");
      return ProtocolType.valueOf(
            dotIndex == -1 ? name : name.substring(dotIndex + 1, name.length()));
   }

   static class JGroupsXmxlConfigurator extends XmlConfigurator {
      protected JGroupsXmxlConfigurator(List<ProtocolConfiguration> protocols) {
         super(copy(protocols));
      }

      static List<ProtocolConfiguration> copy(List<ProtocolConfiguration> protocols) {
         // Make a safe copy of the protocol stack to avoid concurrent modification issues
         List<ProtocolConfiguration> copy =
               new ArrayList<ProtocolConfiguration>(protocols.size());
         for (ProtocolConfiguration p : protocols)
            copy.add(new ProtocolConfiguration(
                  p.getProtocolName(), immutableMapCopy(p.getProperties())));

         return copy;
      }
   }

   static class JGroupsProtocolCfg {
      final Map<ProtocolType, ProtocolConfiguration> protoMap;
      final XmlConfigurator configurator;

      JGroupsProtocolCfg(Map<ProtocolType, ProtocolConfiguration> protoMap,
                         XmlConfigurator configurator) {
         this.protoMap = protoMap;
         this.configurator = configurator;
      }

      JGroupsProtocolCfg addProtocol(ProtocolType type, ProtocolConfiguration cfg, int position) {
         protoMap.put(type, cfg);
         configurator.getProtocolStack().add(position, cfg);
         return this;
      }

      JGroupsProtocolCfg removeProtocol(ProtocolType type) {
         // Update the stack and map
         configurator.getProtocolStack().remove(protoMap.remove(type));
         return this;
      }

      ProtocolConfiguration getProtocol(ProtocolType type) {
         return protoMap.get(type);
      }

      boolean containsProtocol(ProtocolType type) {
         return getProtocol(type) != null;
      }

      JGroupsProtocolCfg replaceProtocol(ProtocolType type, ProtocolConfiguration newCfg) {
         ProtocolConfiguration oldCfg = protoMap.get(type);
         int position = configurator.getProtocolStack().indexOf(oldCfg);
         // Remove protocol and put new configuration in same position
         return removeProtocol(type).addProtocol(type, newCfg, position);
      }

      @Override
      public String toString() {
         return configurator.getProtocolStackString(true);
      }
   }

   enum ProtocolType {
      TCP, TCP_NIO2, UDP, SHARED_LOOPBACK,
      MPING, PING, TCPPING, TEST_PING, SHARED_LOOPBACK_PING,
      MERGE2, MERGE3,
      FD_SOCK, FD, VERIFY_SUSPECT, FD_ALL, FD_ALL2,
      BARRIER,
      UNICAST, UNICAST2, UNICAST3,
      NAKACK, NAKACK2,
      RSVP,
      STABLE,
      GMS,
      UFC, MFC, FC,
      FRAG2,
      STREAMING_STATE_TRANSFER,
      RELAY2,
      TOA
   }

}
