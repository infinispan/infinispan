package org.infinispan.client.hotrod.impl.topology;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.commons.util.Immutables;

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

   public ClusterInfo(String clusterName, List<InetSocketAddress> servers, ClientIntelligence intelligence) {
      this(clusterName, servers, -1, intelligence);
   }

   private ClusterInfo(String clusterName, List<InetSocketAddress> servers, int topologyAge, ClientIntelligence intelligence) {
      this.clusterName = clusterName;
      this.servers = Immutables.immutableListCopy(servers);
      this.topologyAge = topologyAge;
      this.intelligence = Objects.requireNonNull(intelligence);
   }

   public ClusterInfo withTopologyAge(int topologyAge) {
      return new ClusterInfo(clusterName, servers, topologyAge, intelligence);
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

   @Override
   public String toString() {
      return "ClusterInfo{" +
            "name='" + clusterName + '\'' +
            ", servers=" + servers +
            ", age=" + topologyAge +
            ", intelligence=" + intelligence +
            '}';
   }
}
