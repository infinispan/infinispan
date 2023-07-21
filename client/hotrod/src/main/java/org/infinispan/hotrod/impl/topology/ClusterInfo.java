package org.infinispan.hotrod.impl.topology;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;

import org.infinispan.commons.util.Immutables;
import org.infinispan.hotrod.configuration.ClientIntelligence;

/**
 * Cluster definition
 *
 */
public class ClusterInfo {
   private final String clusterName;
   private final List<InetSocketAddress> servers;
   // Topology age provides a way to avoid concurrent cluster view changes,
   // affecting a cluster switch. After a cluster switch, the topology age is
   // increased and so any old requests that might have received topology
   // updates won't be allowed to apply since they refer to older views.
   private final int topologyAge;
   private final ClientIntelligence intelligence;
   private final String sniHostName;

   public ClusterInfo(String clusterName, List<InetSocketAddress> servers, ClientIntelligence intelligence, String sniHostName) {
      this(clusterName, servers, -1, intelligence, sniHostName);
   }

   private ClusterInfo(String clusterName, List<InetSocketAddress> servers, int topologyAge, ClientIntelligence intelligence, String sniHostName) {
      this.clusterName = clusterName;
      this.servers = Immutables.immutableListCopy(servers);
      this.topologyAge = topologyAge;
      this.intelligence = Objects.requireNonNull(intelligence);
      this.sniHostName = sniHostName;
   }

   public ClusterInfo withTopologyAge(int topologyAge) {
      return new ClusterInfo(clusterName, servers, topologyAge, intelligence, sniHostName);
   }

   public String getName() {
      return clusterName;
   }

   public List<InetSocketAddress> getInitialServers() {
      return servers;
   }

   public int getTopologyAge() {
      return topologyAge;
   }

   public ClientIntelligence getIntelligence() {
      return intelligence;
   }

   public String getSniHostName() {
      return sniHostName;
   }

   @Override
   public String toString() {
      return "ClusterInfo{" +
            "name='" + clusterName + '\'' +
            ", servers=" + servers +
            ", age=" + topologyAge +
            ", intelligence=" + intelligence +
            ", sniHostName=" + sniHostName +
            '}';
   }
}
