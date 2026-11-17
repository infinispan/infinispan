package org.infinispan.test.fwk;

import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.FD;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.FD_ALL;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.FD_ALL2;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.FD_ALL3;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.FD_SOCK;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.FD_SOCK2;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.GMS;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.LOCAL_PING;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.MERGE3;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.MPING;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.PING;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.TCP;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.TCP_NIO2;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.TEST_RELAY2;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.UDP;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.VERIFY_SUSPECT;
import static org.infinispan.test.fwk.JGroupsConfigBuilder.ProtocolType.VERIFY_SUSPECT2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
   public static final int BASE_TCP_PORT = 7900;
   public static final int MAX_NODES_PER_SITE = 50;
   private static final int MAX_SITES_PER_THREAD = 4;
   /**
    * The first multicast port of the UDP test stacks. Each cluster gets the port {@code MCAST_PORT_BASE + offset}
    * together with the group {@code 239.10.10.<offset>}, so offsets, and the ports, are limited to 256.
    * <p>
    * The ports are kept below the ephemeral port range of the operating system ({@code 32768-60999} by default on
    * Linux, {@code 49152-65535} on others).
    * </p>
    */
   static final int MCAST_PORT_BASE = 32000;
   /**
    * The last multicast port the UDP test stacks can use, matching the last group {@code 239.10.10.255}.
    */
   static final int MCAST_PORT_LAST = MCAST_PORT_BASE + 255;
   public static final int MAX_NODES_PER_THREAD = MAX_NODES_PER_SITE * MAX_SITES_PER_THREAD;

   public static final String JGROUPS_STACK;
   private static final Map<String, ProtocolStackConfigurator> protocolStackConfigurator = new HashMap<>();

   private static final ThreadLocal<Integer> threadTcpIndex = new ThreadLocal<>() {
      private final AtomicInteger counter = new AtomicInteger(0);

      @Override
      protected Integer initialValue() {
         return counter.getAndIncrement();
      }
   };

   /**
    * Holds unique mcast_addr for each thread used for JGroups channel construction.
    */
   private static final ThreadLocal<Integer> threadUdpIndex = new ThreadLocal<>() {
      private final AtomicInteger counter = new AtomicInteger(0);

      @Override
      protected Integer initialValue() {
         return counter.getAndIncrement();
      }
   };

   private static final ConcurrentMap<String, Integer> testUdpIndex = new ConcurrentHashMap<>();

   static {
      JGROUPS_STACK = LegacyKeySupportSystemProperties.getProperty("infinispan.cluster.stack", "infinispan.test.jgroups.protocol", "test-udp");
      System.out.println("Transport protocol stack used = " + JGROUPS_STACK);
      // Load the XML just once
      protocolStackConfigurator.put("test-tcp", loadProtocolStack("stacks/tcp.xml"));
      protocolStackConfigurator.put("test-udp", loadProtocolStack("stacks/udp.xml"));
      protocolStackConfigurator.put("tcp", loadProtocolStack("org/infinispan/configuration/default-jgroups-tcp.xml"));
      protocolStackConfigurator.put("udp", loadProtocolStack("org/infinispan/configuration/default-jgroups-udp.xml"));
   }

   public static String getJGroupsConfig(String fullTestName, TransportFlags flags) {
      // With the XML already parsed, make a safe copy of the
      // protocol stack configurator and use that accordingly.
      JGroupsProtocolCfg jgroupsCfg = getJGroupsProtocolCfg(getConfigurator().getProtocolStack());

      if (!flags.withFD())
         removeFailureDetection(jgroupsCfg);
      // sockets won't be unexpectedly closed.
      removeFD_SOCK(jgroupsCfg);

      if (!flags.isRelayRequired()) {
         removeRelay2(jgroupsCfg);
      } else {
         ProtocolConfiguration protocol = jgroupsCfg.getProtocol(TEST_RELAY2);
         protocol.getProperties().put("site", flags.siteName());
         if (flags.relayConfig() != null) //if not specified, use default
            protocol.getProperties().put("config", flags.relayConfig());
      }

      if (!flags.withMerge()) {
         removeMerge(jgroupsCfg);
      }

      replacePing(jgroupsCfg);
      int siteIndex = flags.siteIndex();
      if (siteIndex > MAX_SITES_PER_THREAD) {
         throw new IllegalStateException("Currently we only support " + MAX_SITES_PER_THREAD + " ranges/sites!");
      }
      replaceTcpStartPort(jgroupsCfg, siteIndex);
      configurePortRange(jgroupsCfg, siteIndex);
      replaceMCastAddressAndPort(jgroupsCfg, fullTestName, siteIndex);
      configureGMSTimeout(jgroupsCfg, flags);
      return jgroupsCfg.toString();
   }

   public static ProtocolStackConfigurator getConfigurator() {
      ProtocolStackConfigurator stack = protocolStackConfigurator.get(JGROUPS_STACK);
      if (stack == null) {
         throw new IllegalStateException("Unknown protocol stack : " + JGROUPS_STACK);
      }
      return stack;
   }

   private static void replacePing(JGroupsProtocolCfg jgroupsCfg) {
      if (!jgroupsCfg.containsProtocol(LOCAL_PING)) {
         ProtocolType[] pingProtocols = new ProtocolType[]{MPING, PING};
         ProtocolType pingProtocolToBeReplaced = null;
         for (ProtocolType protocol : pingProtocols) {
            if (jgroupsCfg.containsProtocol(protocol)) {
               pingProtocolToBeReplaced = protocol;
               break;
            }
         }
         if (pingProtocolToBeReplaced == null) {
            throw new IllegalStateException("Can't replace the ping protocol with LOCAL_PING");
         }
         jgroupsCfg.replaceProtocol(pingProtocolToBeReplaced,
               new ProtocolConfiguration(LOCAL_PING.name(), new HashMap<>()));
      }
   }

   private static void removeMerge(JGroupsProtocolCfg jgroupsCfg) {
      jgroupsCfg.removeProtocol(MERGE3);
   }

   /**
    * Remove all failure detection related
    * protocols from the given JGroups stack.
    */
   private static void removeFailureDetection(JGroupsProtocolCfg jgroupsCfg) {
      jgroupsCfg.removeProtocol(FD).removeProtocol(FD_SOCK).removeProtocol(FD_SOCK2)
                .removeProtocol(FD_ALL).removeProtocol(FD_ALL2).removeProtocol(FD_ALL3)
                .removeProtocol(VERIFY_SUSPECT).removeProtocol(VERIFY_SUSPECT2);
   }

   private static void removeFD_SOCK(JGroupsProtocolCfg jgroupsCfg) {
      jgroupsCfg.removeProtocol(FD_SOCK).removeProtocol(FD_SOCK2);
   }

   private static void removeRelay2(JGroupsProtocolCfg jgroupsCfg) {
      jgroupsCfg.removeProtocol(TEST_RELAY2);
   }

   private static void replaceMCastAddressAndPort(JGroupsProtocolCfg jgroupsCfg, String fullTestName, int siteIndex) {
      ProtocolConfiguration udp = jgroupsCfg.getProtocol(UDP);
      if (udp == null) {
         return;
      }

      Integer udpIndex = testUdpIndex.computeIfAbsent(fullTestName, k -> threadUdpIndex.get());

      if (siteIndex < 0) {
         siteIndex = 0;
      }
      int clusterOffset = udpIndex * MAX_SITES_PER_THREAD + siteIndex;
      // Each cluster needs its own group and port, we can't wrap the ports and possibly join different clusters.
      if (clusterOffset > MCAST_PORT_LAST - MCAST_PORT_BASE)
         throw new IllegalStateException("UDP cluster offset " + clusterOffset + " exceeds the "
               + (MCAST_PORT_LAST - MCAST_PORT_BASE + 1) + " multicast groups available to tests");

      Map<String, String> props = udp.getProperties();
      props.put("mcast_addr", "239.10.10." + clusterOffset);
      props.put("mcast_port", String.valueOf(MCAST_PORT_BASE + clusterOffset));
      replaceProperties(jgroupsCfg, props, UDP);
   }

   private static void replaceTcpStartPort(JGroupsProtocolCfg jgroupsCfg, int siteIndex) {
      ProtocolType transportProtocol = jgroupsCfg.transportType;
      if (transportProtocol != TCP && transportProtocol != TCP_NIO2) {
         return;
      }

      Map<String, String> props = jgroupsCfg.getProtocol(transportProtocol).getProperties();
      Integer threadIndex = threadTcpIndex.get();
      int startPort;
      if (siteIndex >= 0) {
         // Site index set
         startPort = BASE_TCP_PORT + threadIndex * MAX_NODES_PER_THREAD + siteIndex * MAX_NODES_PER_SITE;
      } else {
         // Site index not set, use the thread's full range
         startPort = BASE_TCP_PORT + threadIndex * MAX_NODES_PER_THREAD;
      }
      props.put("bind_port", String.valueOf(startPort));
      replaceProperties(jgroupsCfg, props, transportProtocol);
   }

   private static void configurePortRange(JGroupsProtocolCfg jgroupsCfg, int siteIndex) {
      ProtocolType transportProtocol = jgroupsCfg.transportType;
      Map<String, String> props = jgroupsCfg.getProtocol(transportProtocol).getProperties();
      int portRange;
      if (siteIndex >= 0) {
         // Site index set
         portRange = MAX_NODES_PER_SITE;
      } else {
         portRange = MAX_NODES_PER_THREAD;
      }

      // In JGroups, the port_range is inclusive
      props.put("port_range", String.valueOf(portRange - 1));
      replaceProperties(jgroupsCfg, props, transportProtocol);
   }

   private static void configureGMSTimeout(JGroupsProtocolCfg jgroupsCfg, TransportFlags flags) {
      if (!flags.isZeroJoinTimeout()) {
         return;
      }
      var protocol = jgroupsCfg.getProtocol(GMS);
      if (protocol == null) {
         return;
      }
      protocol.getProperties().put("join_timeout", "0");
   }

   private static void replaceProperties(
         JGroupsProtocolCfg cfg, Map<String, String> newProps, ProtocolType type) {
      ProtocolConfiguration protocol = cfg.getProtocol(type);
      ProtocolConfiguration newProtocol =
            new ProtocolConfiguration(protocol.getProtocolName(), newProps);
      cfg.replaceProtocol(type, newProtocol);
   }

   private static ProtocolStackConfigurator loadProtocolStack(String relativePath) {
      try {
         return ConfiguratorFactory.getStackConfigurator(relativePath);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private static JGroupsProtocolCfg getJGroupsProtocolCfg(List<ProtocolConfiguration> baseStack) {
      JGroupsXmxlConfigurator configurator = new JGroupsXmxlConfigurator(baseStack);
      List<ProtocolConfiguration> protoStack = configurator.getProtocolStack();
      ProtocolType transportType = getProtocolType(protoStack.get(0).getProtocolName());
      Map<ProtocolType, ProtocolConfiguration> protoMap = new HashMap<>(protoStack.size());
      for (ProtocolConfiguration cfg : protoStack) {
         protoMap.put(getProtocolType(cfg.getProtocolName()), cfg);
      }

      return new JGroupsProtocolCfg(protoMap, configurator, transportType);
   }

   /**
    * Returns the multicast endpoint a JGroups UDP stack binds.
    * <p>
    * Used to name the endpoint when a cache manager fails to start, so the method never throws.
    * </p>
    *
    * @param stack the stack as XML, as set in
    *              {@link org.infinispan.remoting.transport.jgroups.JGroupsTransport#CONFIGURATION_STRING}, or
    *              {@code null} when the cache manager configures its stack another way
    * @return {@code mcast_addr:mcast_port} of the stack's {@code UDP} protocol, or {@code "unknown"} if the stack is
    *         missing, can't be parsed or has no {@code UDP} protocol
    */
   static String mcastEndpoint(String stack) {
      if (stack == null) {
         return "unknown";
      }
      try {
         // Parsed the same way JGroupsTransport parses it when creating the channel
         XmlConfigurator configurator = XmlConfigurator.getInstance(
               new ByteArrayInputStream(stack.getBytes(StandardCharsets.UTF_8)));
         for (ProtocolConfiguration protocol : configurator.getProtocolStack()) {
            if (getProtocolType(protocol.getProtocolName()) == UDP) {
               Map<String, String> properties = protocol.getProperties();
               return properties.get("mcast_addr") + ":" + properties.get("mcast_port");
            }
         }
         return "unknown";
      } catch (IOException | RuntimeException e) {
         // getProtocolType throws for protocols outside ProtocolType, the parser for malformed stacks
         return "unknown (" + e + ")";
      }
   }

   private static ProtocolType getProtocolType(String name) {
      int dotIndex = name.lastIndexOf(".");
      return ProtocolType.valueOf(dotIndex == -1 ? name : name.substring(dotIndex + 1));
   }

   static class JGroupsXmxlConfigurator extends XmlConfigurator {
      protected JGroupsXmxlConfigurator(List<ProtocolConfiguration> protocols) {
         super(copy(protocols));
      }

      static List<ProtocolConfiguration> copy(List<ProtocolConfiguration> protocols) {
         // Make a safe copy of the protocol stack to avoid concurrent modification issues
         List<ProtocolConfiguration> copy = new ArrayList<>(protocols.size());
         for (ProtocolConfiguration p : protocols) {
            copy.add(new ProtocolConfiguration(p.getProtocolName(), Map.copyOf(p.getProperties())));
         }

         return copy;
      }
   }

   static class JGroupsProtocolCfg {
      final Map<ProtocolType, ProtocolConfiguration> protoMap;
      final XmlConfigurator configurator;
      final ProtocolType transportType;

      JGroupsProtocolCfg(Map<ProtocolType, ProtocolConfiguration> protoMap,
                         XmlConfigurator configurator,
                         ProtocolType transportType) {
         this.protoMap = protoMap;
         this.configurator = configurator;
         this.transportType = transportType;
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
      RED,
      MPING, PING, TCPPING, LOCAL_PING, SHARED_LOOPBACK_PING,
      MERGE2, MERGE3,
      FD_SOCK, FD, VERIFY_SUSPECT, VERIFY_SUSPECT2, FD_ALL, FD_ALL2, FD_ALL3, FD_SOCK2,
      BARRIER,
      UNICAST, UNICAST2, UNICAST3,
      NAKACK, NAKACK2,
      RSVP,
      STABLE,
      GMS,
      UFC, MFC, FC, UFC_NB, MFC_NB,
      FRAG2, FRAG3, FRAG4,
      STREAMING_STATE_TRANSFER,
      TEST_RELAY2
   }

}
