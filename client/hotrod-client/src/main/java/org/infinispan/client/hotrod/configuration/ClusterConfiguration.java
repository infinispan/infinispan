package org.infinispan.client.hotrod.configuration;

import java.util.List;

/**
 * @since 8.1
 */
public class ClusterConfiguration {
   private final List<ServerConfiguration> serverCluster;
   private final String clusterName;

   public ClusterConfiguration(List<ServerConfiguration> serverCluster, String clusterName) {
      this.serverCluster = serverCluster;
      this.clusterName = clusterName;
   }

   public List<ServerConfiguration> getCluster() {
      return serverCluster;
   }

   public String getClusterName() {
      return clusterName;
   }
}
