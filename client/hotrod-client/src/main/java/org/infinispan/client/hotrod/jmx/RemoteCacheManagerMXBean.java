package org.infinispan.client.hotrod.jmx;

/**
 * RemoteCacheManager client-side statistics and operations
 */
public interface RemoteCacheManagerMXBean {
   /**
    * Returns a list of servers to which the client is currently connected in the format of ip_address:port_number.
    */
   String[] getServers();

   /**
    * Returns the number of active connections
    */
   int getActiveConnectionCount();

   /**
    * Returns the total number of connections
    */
   int getConnectionCount();

   /**
    * Returns the number of idle connections
    */
   int getIdleConnectionCount();

   /**
    * Switch remote cache manager to a different cluster, previously
    * declared via configuration. If the switch was completed successfully,
    * this method returns {@code true}, otherwise it returns {@code false}.
    *
    * @param clusterName name of the cluster to which to switch to
    * @return {@code true} if the cluster was switched, {@code false} otherwise
    */
   boolean switchToCluster(String clusterName);

   /**
    * Switch remote cache manager to a the default cluster, previously
    * declared via configuration. If the switch was completed successfully,
    * this method returns {@code true}, otherwise it returns {@code false}.
    *
    * @return {@code true} if the cluster was switched, {@code false} otherwise
    */
   boolean switchToDefaultCluster();
}
