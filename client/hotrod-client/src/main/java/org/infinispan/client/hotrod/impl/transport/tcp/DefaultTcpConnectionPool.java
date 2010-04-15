package org.infinispan.client.hotrod.impl.transport.tcp;

import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.apache.commons.pool.impl.GenericObjectPoolFactory;
import org.infinispan.client.hotrod.impl.transport.TransportException;
import org.infinispan.client.hotrod.impl.transport.VHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Properties;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 *
 * todo - all methods but init and start can be called from multiple threads, add proper sync
 */
public class DefaultTcpConnectionPool implements TcpConnectionPool {

   private static Log log = LogFactory.getLog(DefaultTcpConnectionPool.class);

   private GenericKeyedObjectPool connectionPool;
   private PropsKeyedObjectPoolFactory poolFactory;
   private RequestBalancingStrategy balancer;
   private Collection<InetSocketAddress> servers;

   @Override
   public void init(Properties props) {
      String balancerClass = props.getProperty("requestBalancingStrategy", RoundRobinBalancingStrategy.class.getName());
      balancer = (RequestBalancingStrategy) VHelper.newInstance(balancerClass);
      poolFactory = new PropsKeyedObjectPoolFactory(new TcpConnectionFactory(), props);
   }

   @Override
   public void start(Collection<InetSocketAddress> servers) {
      connectionPool = (GenericKeyedObjectPool) poolFactory.createPool();
      balancer.setServers(servers);
      this.servers = servers;
   }

   @Override
   public Socket getConnection() {
      InetSocketAddress server = balancer.nextServer();
      try {
         return (Socket) connectionPool.borrowObject(server);
      } catch (Exception e) {
         String message = "Could not fetch connection";
         log.error(message, e);
         throw new TransportException(message, e);
      }
   }

   @Override
   public void releaseConnection(Socket socket) {
      SocketAddress remoteAddress = socket.getRemoteSocketAddress();
      if (!servers.contains(remoteAddress)) throw new IllegalStateException(remoteAddress.toString());
      try {
         connectionPool.returnObject(remoteAddress, socket);
      } catch (Exception e) {
         log.warn("Could not release connection",e);
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

   @Override
   public void destroy() {
      connectionPool.clear();
      try {
         connectionPool.close();
      } catch (Exception e) {
         log.warn("Exception while shutting down the connection pool.", e);
      }
   }
}
