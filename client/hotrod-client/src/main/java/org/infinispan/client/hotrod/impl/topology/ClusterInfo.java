package org.infinispan.client.hotrod.impl.topology;

import java.net.InetSocketAddress;
import java.util.List;

import org.infinispan.commons.util.Immutables;

/**
 * Cluster definition
 *
 * @author Dan Berindei
 */
public class ClusterInfo {
   private final String clusterName;
   private final List<InetSocketAddress> servers;

   public ClusterInfo(String clusterName, List<InetSocketAddress> servers) {
      this.clusterName = clusterName;
      this.servers = Immutables.immutableListCopy(servers);
   }

   public String getName() {
      return clusterName;
   }

   public List<InetSocketAddress> getInitialServers() {
      return servers;
   }

   @Override
   public String toString() {
      return "ClusterInfo{" +
             "name='" + clusterName + '\'' +
             ", servers=" + servers +
             '}';
   }
}
