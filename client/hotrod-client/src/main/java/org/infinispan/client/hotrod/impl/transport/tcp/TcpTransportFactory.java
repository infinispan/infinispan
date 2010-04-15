package org.infinispan.client.hotrod.impl.transport.tcp;

import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.infinispan.client.hotrod.impl.Transport;
import org.infinispan.client.hotrod.impl.TransportFactory;
import org.infinispan.client.hotrod.impl.transport.TransportException;
import org.infinispan.client.hotrod.impl.transport.VHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * todo - all methods but start and start can be called from multiple threads, add proper sync
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TcpTransportFactory implements TransportFactory {

   private static Log log = LogFactory.getLog(TcpTransportFactory.class);

   private volatile GenericKeyedObjectPool connectionPool;
   private PropsKeyedObjectPoolFactory poolFactory;
   private RequestBalancingStrategy balancer;
   private Collection<InetSocketAddress> servers;

   @Override
   public void start(Properties props, Collection<InetSocketAddress> staticConfiguredServers) {
      servers = staticConfiguredServers;
      String balancerClass = props.getProperty("requestBalancingStrategy", RoundRobinBalancingStrategy.class.getName());
      balancer = (RequestBalancingStrategy) VHelper.newInstance(balancerClass);
      poolFactory = new PropsKeyedObjectPoolFactory(new TcpConnectionFactory(), props);
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
   public Transport getTransport() {
      InetSocketAddress server = balancer.nextServer();
      try {
         return (Transport) connectionPool.borrowObject(server);
      } catch (Exception e) {
         String message = "Could not fetch transport";
         log.error(message, e);
         throw new TransportException(message, e);
      }
   }

   @Override
   public void releaseTransport(Transport transport) {
      TcpTransport tcpTransport = (TcpTransport) transport;
      try {
         connectionPool.returnObject(tcpTransport.getServerAddress(), tcpTransport);
      } catch (Exception e) {
         log.warn("Could not release connection: " + tcpTransport,e);
      }
   }

   @Override
   public void updateServers(Collection<InetSocketAddress> newServers) {
      if (newServers.containsAll(servers) && servers.containsAll(newServers)) {
         log.info("Same list of servers, not changing the pool");
         return;
      }
      for (InetSocketAddress server : newServers) {
         if (!servers.contains(server)) {
            log.info("New server added(" + server + "), adding to the pool.");
            try {
               connectionPool.addObject(server);
            } catch (Exception e) {
               log.warn("Failed adding server " + server, e);
            }
         }
      }
      for (InetSocketAddress server : servers) {
         if (!newServers.contains(server)) {
            log.info("Server not in cluster anymore(" + server + "), removing from the pool.");
            connectionPool.clear(server);
         }
      }
      servers.clear();
      servers.addAll(newServers);
   }

}
