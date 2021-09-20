package org.infinispan.client.hotrod.impl.topology;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
   // This completable future is completed when we switch away from the cluster
   private final CompletableFuture<ClusterInfo> clusterSwitchFuture = new CompletableFuture<>();

   public ClusterInfo(String clusterName, List<InetSocketAddress> servers) {
      this(clusterName, servers, -1);
   }

   private ClusterInfo(String clusterName, List<InetSocketAddress> servers, int topologyAge) {
      this.clusterName = clusterName;
      this.servers = Immutables.immutableListCopy(servers);
      this.topologyAge = topologyAge;
   }

   public ClusterInfo withTopologyAge(int topologyAge) {
      return new ClusterInfo(clusterName, servers, topologyAge);
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

   public CompletableFuture<ClusterInfo> getClusterSwitchFuture() {
      return clusterSwitchFuture;
   }

   @Override
   public String toString() {
      return "ClusterInfo{" +
             "name='" + clusterName + '\'' +
             ", servers=" + servers +
             ", age=" + topologyAge +
             '}';
   }
}
