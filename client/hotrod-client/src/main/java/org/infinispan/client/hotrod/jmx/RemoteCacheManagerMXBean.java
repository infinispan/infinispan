package org.infinispan.client.hotrod.jmx;

public interface RemoteCacheManagerMXBean {
   /**
    * Returns the list of servers the client is currently connected to in the format address:port
    */
   String[] getServers();

   int getActiveConnectionCount();

   int getConnectionCount();

   int getIdleConnectionCount();
}
