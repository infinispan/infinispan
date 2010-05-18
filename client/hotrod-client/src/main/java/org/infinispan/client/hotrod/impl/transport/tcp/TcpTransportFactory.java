package org.infinispan.client.hotrod.impl.transport.tcp;

import net.jcip.annotations.ThreadSafe;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.impl.transport.VHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@ThreadSafe
public class TcpTransportFactory implements TransportFactory {

   private static final Log log = LogFactory.getLog(TcpTransportFactory.class);

   /**
    * These are declared volatile as the thread that calls {@link org.infinispan.client.hotrod.impl.transport.TransportFactory#start(java.util.Properties, java.util.Collection, java.util.concurrent.atomic.AtomicInteger)}
    * might(and likely will) be different from the thread(s) that calls {@link #getTransport()} or other methods
    */
   private volatile GenericKeyedObjectPool connectionPool;
   private volatile RequestBalancingStrategy balancer;
   private volatile Collection<InetSocketAddress> servers;
   private volatile ConsistentHash consistentHash;
   private final ConsistentHashFactory hashFactory = new ConsistentHashFactory();

   @Override
   public void start(Properties props, Collection<InetSocketAddress> staticConfiguredServers, AtomicInteger topologyId) {
      hashFactory.init(props);
      servers = staticConfiguredServers;
      String balancerClass = props.getProperty("request-balancing-strategy", RoundRobinBalancingStrategy.class.getName());
      balancer = (RequestBalancingStrategy) VHelper.newInstance(balancerClass);
      PropsKeyedObjectPoolFactory poolFactory = new PropsKeyedObjectPoolFactory(new TransportObjectFactory(this, topologyId), props);
      connectionPool = (GenericKeyedObjectPool) poolFactory.createPool();
      balancer.setServers(servers);
   }

   @Override
   public void destroy() {
      connectionPool.clear();
      try {
         connectionPool.close();
      } catch (Exception e) {
         log.warn("Exception while shutting down the connection pool.", e);
      }
   }

   @Override
   public void updateHashFunction(LinkedHashMap<InetSocketAddress,Integer> servers2HashCode, int numKeyOwners, short hashFunctionVersion, int hashSpace) {
      ConsistentHash hash = hashFactory.newConsistentHash(hashFunctionVersion);
      if (hash == null) {
         log.warn("No hash function configured for version: " + hashFunctionVersion);
      } else {
         hash.init(servers2HashCode, numKeyOwners, hashSpace);
      }
      consistentHash = hash;
   }

   @Override
   public Transport getTransport() {
      InetSocketAddress server = balancer.nextServer();
      return borrowTransportFromPool(server);
   }

   public Transport getTransport(byte[] key) {
      InetSocketAddress server;
      if (consistentHash != null) {
         server = consistentHash.getServer(key);
         if (log.isTraceEnabled()) {
            log.trace("Using consistent hash for determining the server: " + server);
         }
      } else {
         server = balancer.nextServer();
         if (log.isTraceEnabled()) {
            log.trace("Using the balancer for determining the server: " + server);
         }
      }
      return borrowTransportFromPool(server);
   }

   @Override
   public void releaseTransport(Transport transport) {
      TcpTransport tcpTransport = (TcpTransport) transport;
      try {
         connectionPool.returnObject(tcpTransport.getServerAddress(), tcpTransport);
      } catch (Exception e) {
         log.warn("Could not release connection: " + tcpTransport, e);
      } finally {
         logConnectionInfo(tcpTransport.getServerAddress());
      }
   }

   @Override
   public void updateServers(Collection<InetSocketAddress> newServers) {
      synchronized (this) {//only one updateServers at a time. 
         Set<InetSocketAddress> addedServers = new HashSet<InetSocketAddress>(newServers);
         addedServers.removeAll(servers);
         Set<InetSocketAddress> failedServers = new HashSet<InetSocketAddress>(servers);
         failedServers.removeAll(newServers);
         if (log.isTraceEnabled()) {
            log.trace("Current list: " + servers);
            log.trace("New list: " + newServers);
            log.trace("Added servers: " + addedServers);
            log.trace("Removed servers: " + failedServers);
         }
         if (failedServers.isEmpty() && newServers.isEmpty()) {
            log.info("Same list of servers, not changing the pool");
            return;
         }

         //1. first add new servers. For servers that went down, the returned transport will fail for now
         for (InetSocketAddress server : newServers) {
            log.info("New server added(" + server + "), adding to the pool.");
            try {
               connectionPool.addObject(server);
            } catch (Exception e) {
               log.warn("Failed adding new server " + server, e);
            }
         }

         //2. now set the server list to the active list of servers. All the active servers (potentially together with some
         // failed servers) are in the pool now. But after this, the pool won't be asked for connections to failed servers,
         // as the balancer will only know about the active servers
         balancer.setServers(newServers);


         //3. Now just remove failed servers
         for (InetSocketAddress server : failedServers) {
            log.info("Server not in cluster anymore(" + server + "), removing from the pool.");
            connectionPool.clear(server);
         }

         servers.clear();
         servers.addAll(newServers);
      }
   }

   public Collection<InetSocketAddress> getServers() {
      return servers;
   }

   private void logConnectionInfo(InetSocketAddress server) {
      if (log.isTraceEnabled()) {
         log.trace("For server " + server + ": active = " + connectionPool.getNumActive(server) + "; idle = " + connectionPool.getNumIdle(server));
      }
   }

   private Transport borrowTransportFromPool(InetSocketAddress server) {
      try {
         return (Transport) connectionPool.borrowObject(server);
      } catch (Exception e) {
         String message = "Could not fetch transport";
         log.error(message, e);
         throw new TransportException(message, e);
      } finally {
         logConnectionInfo(server);
      }
   }

   public ConsistentHash getConsistentHash() {
      return consistentHash;
   }
}
