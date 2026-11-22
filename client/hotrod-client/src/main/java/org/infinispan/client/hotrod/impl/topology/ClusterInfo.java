package org.infinispan.client.hotrod.impl.topology;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;

/**
 * Cluster definition
 *
 * @author Dan Berindei
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
      this.servers = List.copyOf(servers);
      this.topologyAge = topologyAge;
      this.intelligence = Objects.requireNonNull(intelligence);
      this.sniHostName = sniHostName;
   }

   public ClusterInfo withTopologyAge(int topologyAge) {
      return new ClusterInfo(clusterName, servers, topologyAge, intelligence, sniHostName);
   }

   /**
    * Creates a new ClusterInfo with degraded intelligence level.
    * Used when AUTO mode needs to degrade from HASH_DISTRIBUTION_AWARE to BASIC.
    *
    * @param degradedIntelligence the degraded intelligence level
    * @return a new ClusterInfo with the degraded intelligence
    */
   public ClusterInfo withDegradedIntelligence(ClientIntelligence degradedIntelligence) {
      return new ClusterInfo(clusterName, servers, topologyAge, degradedIntelligence, sniHostName);
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

   /**
    * Returns the configured intelligence, which may be AUTO.
    *
    * @return the configured client intelligence
    */
   public ClientIntelligence getConfiguredIntelligence() {
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
            ", sniHostname=" + sniHostName +
            '}';
   }
}
