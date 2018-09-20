package org.infinispan.client.hotrod.jmx;

public interface RemoteCacheManagerMXBean {
   /**
    * Returns a list of servers to which the client is currently connected in the format of ip_address:port_number.
    */
   String[] getServers();

   int getActiveConnectionCount();

   int getConnectionCount();

   int getIdleConnectionCount();
}
