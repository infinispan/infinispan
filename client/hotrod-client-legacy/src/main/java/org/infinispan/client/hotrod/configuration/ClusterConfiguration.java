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
   private final String sniHostName;

   public ClusterConfiguration(List<ServerConfiguration> serverCluster, String clusterName, ClientIntelligence intelligence, String sniHostName) {
      this.serverCluster = serverCluster;
      this.clusterName = clusterName;
      this.intelligence = intelligence;
      this.sniHostName = sniHostName;
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

   public String sniHostName() {
      return sniHostName;
   }

   @Override
   public String toString() {
      return "ClusterConfiguration{" +
            "serverCluster=" + Util.toStr(serverCluster) +
            ", clusterName='" + clusterName + '\'' +
            ", intelligence=" + intelligence +
            ", sniHostName=" + sniHostName +
            '}';
   }
}
