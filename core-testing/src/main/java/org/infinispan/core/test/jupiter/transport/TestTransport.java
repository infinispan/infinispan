package org.infinispan.core.test.jupiter.transport;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;

/**
 * Configures JGroups transport for test clusters with automatic isolation.
 * <p>
 * Each cluster gets a unique cluster name and port range to enable
 * parallel test execution without interference.
 *
 * @since 16.2
 */
public final class TestTransport {

   private static final AtomicInteger CLUSTER_COUNTER = new AtomicInteger(0);
   private static final int BASE_MCAST_PORT = 46755;
   private static final int BASE_TCP_PORT = 7900;
   private static final int MAX_NODES_PER_CLUSTER = 50;

   private TestTransport() {
   }

   /**
    * Allocates a unique port offset for a new cluster.
    */
   public static int allocatePortOffset() {
      return CLUSTER_COUNTER.getAndIncrement() * MAX_NODES_PER_CLUSTER;
   }

   /**
    * Configures the global configuration builder with clustered transport
    * suitable for testing.
    *
    * @param gcb            the global configuration builder
    * @param clusterName    unique cluster name for isolation
    * @param transportStack the JGroups stack to use (e.g. "test-udp", "test-tcp")
    * @param nodeIndex      index of the node within the cluster
    * @param portOffset     shared port offset for this cluster (from {@link #allocatePortOffset()})
    */
   public static void configure(GlobalConfigurationBuilder gcb, String clusterName,
                                String transportStack, int nodeIndex, int portOffset) {
      gcb.transport().defaultTransport()
            .clusterName(clusterName)
            .nodeName(clusterName + "-" + nodeIndex)
            .addProperty(JGroupsTransport.CONFIGURATION_STRING,
                  buildStack(transportStack, portOffset));
   }

   /**
    * Configures the global configuration builder with clustered transport
    * that includes RELAY2 for cross-site communication.
    *
    * @param gcb               the global configuration builder
    * @param clusterName       unique cluster name for this site's internal cluster
    * @param siteName          this node's site name
    * @param bridgeClusterName unique name for the RELAY2 bridge cluster
    * @param nodeIndex         index of the node within the site
    * @param portOffset        shared port offset for this site
    */
   public static void configureXSite(GlobalConfigurationBuilder gcb, String clusterName,
                                     String siteName, String bridgeClusterName,
                                     int nodeIndex, int portOffset) {
      gcb.transport().defaultTransport()
            .clusterName(clusterName)
            .nodeName(clusterName + "-" + nodeIndex)
            .addProperty(JGroupsTransport.CONFIGURATION_STRING,
                  buildXSiteStack(portOffset, siteName, bridgeClusterName));
   }

   private static String buildStack(String baseStack, int portOffset) {
      if (baseStack.contains("tcp")) {
         return tcpStack(BASE_TCP_PORT + portOffset);
      } else {
         return udpStack(BASE_MCAST_PORT + portOffset);
      }
   }

   private static String buildXSiteStack(int portOffset, String siteName, String bridgeClusterName) {
      int mcastPort = BASE_MCAST_PORT + portOffset;
      return """
            <config xmlns="urn:org:jgroups"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xsi:schemaLocation="urn:org:jgroups http://www.jgroups.org/schema/jgroups.xsd">
              <UDP bind_addr="127.0.0.1"
                   bind_port="0"
                   mcast_addr="239.6.7.8"
                   mcast_port="%d"
                   port_range="10"
                   bundler_type="transfer-queue"
                   use_vthreads="true" />
              <LOCAL_PING />
              <MERGE3 min_interval="1000" max_interval="3000" />
              <FD_ALL3 timeout="3000" interval="1000" />
              <VERIFY_SUSPECT2 timeout="1000" />
              <pbcast.NAKACK2 />
              <UNICAST3 />
              <pbcast.STABLE desired_avg_gossip="2000" max_bytes="1M" />
              <pbcast.GMS join_timeout="2000" print_local_addr="false" />
              <UFC max_credits="4M" min_threshold="0.4" />
              <MFC max_credits="4M" min_threshold="0.4" />
              <FRAG4 />
              <org.infinispan.core.test.jupiter.transport.TestRelay
                   site="%s"
                   bridge_name="%s"
                   relay_multicasts="true"
                   max_site_masters="1" />
            </config>
            """.formatted(mcastPort, siteName, bridgeClusterName);
   }

   private static String tcpStack(int startPort) {
      return """
            <config xmlns="urn:org:jgroups"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xsi:schemaLocation="urn:org:jgroups http://www.jgroups.org/schema/jgroups.xsd">
              <TCP bind_addr="127.0.0.1"
                   bind_port="%d"
                   port_range="50"
                   recv_buf_size="20M"
                   send_buf_size="640K"
                   bundler_type="transfer-queue"
                   use_vthreads="true" />
              <LOCAL_PING />
              <MERGE3 min_interval="1000" max_interval="3000" />
              <FD_ALL3 timeout="3000" interval="1000" />
              <VERIFY_SUSPECT2 timeout="1000" />
              <pbcast.NAKACK2 use_mcast_xmit="false" />
              <UNICAST3 />
              <pbcast.STABLE desired_avg_gossip="2000" max_bytes="1M" />
              <pbcast.GMS join_timeout="2000" print_local_addr="false" />
              <UFC max_credits="4M" min_threshold="0.4" />
              <MFC max_credits="4M" min_threshold="0.4" />
              <FRAG4 />
            </config>
            """.formatted(startPort);
   }

   private static String udpStack(int mcastPort) {
      return """
            <config xmlns="urn:org:jgroups"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xsi:schemaLocation="urn:org:jgroups http://www.jgroups.org/schema/jgroups.xsd">
              <UDP bind_addr="127.0.0.1"
                   bind_port="0"
                   mcast_addr="239.6.7.8"
                   mcast_port="%d"
                   port_range="10"
                   bundler_type="transfer-queue"
                   use_vthreads="true" />
              <LOCAL_PING />
              <MERGE3 min_interval="1000" max_interval="3000" />
              <FD_ALL3 timeout="3000" interval="1000" />
              <VERIFY_SUSPECT2 timeout="1000" />
              <pbcast.NAKACK2 />
              <UNICAST3 />
              <pbcast.STABLE desired_avg_gossip="2000" max_bytes="1M" />
              <pbcast.GMS join_timeout="2000" print_local_addr="false" />
              <UFC max_credits="4M" min_threshold="0.4" />
              <MFC max_credits="4M" min_threshold="0.4" />
              <FRAG4 />
            </config>
            """.formatted(mcastPort);
   }
}
