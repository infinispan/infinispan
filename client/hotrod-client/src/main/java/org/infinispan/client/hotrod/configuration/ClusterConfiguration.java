package org.infinispan.client.hotrod.configuration;

import java.util.List;

import org.infinispan.commons.util.Util;

/**
 * @since 8.1
 */
public class ClusterConfiguration {
   private final List<ServerConfiguration> serverCluster;
   private final String clusterName;
   private final ClientIntelligence intelligence;

   public ClusterConfiguration(List<ServerConfiguration> serverCluster, String clusterName, ClientIntelligence intelligence) {
      this.serverCluster = serverCluster;
      this.clusterName = clusterName;
      this.intelligence = intelligence;
   }

   public List<ServerConfiguration> getCluster() {
      return serverCluster;
   }

   public String getClusterName() {
      return clusterName;
   }

   public ClientIntelligence getClientIntelligence() {
      return intelligence;
   }

   @Override
   public String toString() {
      return "ClusterConfiguration{" +
            "serverCluster=" + Util.toStr(serverCluster) +
            ", clusterName='" + clusterName + '\'' +
            ", intelligence=" + intelligence +
            '}';
   }
}
