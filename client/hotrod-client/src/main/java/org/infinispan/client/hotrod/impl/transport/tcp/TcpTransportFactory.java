package org.infinispan.client.hotrod.impl.transport.tcp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;

import net.jcip.annotations.ThreadSafe;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ServerConfiguration;
import org.infinispan.client.hotrod.configuration.SslConfiguration;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.commons.util.Util;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@ThreadSafe
public class TcpTransportFactory implements TransportFactory {

   private static final Log log = LogFactory.getLog(TcpTransportFactory.class, Log.class);

   /**
    * We need synchronization as the thread that calls {@link org.infinispan.client.hotrod.impl.transport.TransportFactory#start(org.infinispan.client.hotrod.impl.protocol.Codec, org.infinispan.client.hotrod.impl.ConfigurationProperties, java.util.Collection, java.util.concurrent.atomic.AtomicInteger, ClassLoader)}
    * might(and likely will) be different from the thread(s) that calls {@link org.infinispan.client.hotrod.impl.transport.TransportFactory#getTransport(java.util.Set} or other methods
    */
   private final Object lock = new Object();
   // The connection pool implementation is assumed to be thread-safe, so we need to synchronize just the access to this field and not the method calls
   private GenericKeyedObjectPool<SocketAddress, TcpTransport> connectionPool;
   private RequestBalancingStrategy balancer;
   private Collection<SocketAddress> servers;
   private ConsistentHash consistentHash;
   private final ConsistentHashFactory hashFactory = new ConsistentHashFactory();

   // the primitive fields are often accessed separately from the rest so it makes sense not to require synchronization for them
   private volatile boolean tcpNoDelay;
   private volatile int soTimeout;
   private volatile int connectTimeout;
   private volatile int maxRetries;
   private volatile SSLContext sslContext;

   @Override
   public void start(Codec codec, Configuration configuration, AtomicInteger topologyId) {
      synchronized (lock) {
         hashFactory.init(configuration);
         boolean pingOnStartup = configuration.pingOnStartup();
         servers = new ArrayList<SocketAddress>();
         for(ServerConfiguration server : configuration.servers()) {
            servers.add(new InetSocketAddress(server.host(), server.port()));
         }
         servers = Collections.unmodifiableCollection(servers);
         balancer = Util.getInstance(configuration.balancingStrategy());
         tcpNoDelay = configuration.tcpNoDelay();
         soTimeout = configuration.socketTimeout();
         connectTimeout = configuration.connectionTimeout();
         maxRetries = configuration.maxRetries();

         if (configuration.ssl().enabled()) {
            SslConfiguration ssl = configuration.ssl();
            if (ssl.sslContext() != null) {
               sslContext = ssl.sslContext();
            } else {
               sslContext = SslContextFactory.getContext(ssl.keyStoreFileName(), ssl.keyStorePassword(), ssl.trustStoreFileName(), ssl.trustStorePassword());
            }
         }

         if (log.isDebugEnabled()) {
            log.debugf("Statically configured servers: %s", servers);
            log.debugf("Load balancer class: %s", balancer.getClass().getName());
            log.debugf("Tcp no delay = %b; client socket timeout = %d ms; connect timeout = %d ms",
                       tcpNoDelay, soTimeout, connectTimeout);
         }
         PropsKeyedObjectPoolFactory<SocketAddress, TcpTransport> poolFactory =
               new PropsKeyedObjectPoolFactory<SocketAddress, TcpTransport>(
                     new TransportObjectFactory(codec, this, topologyId, pingOnStartup),
                     configuration.connectionPool());
         createAndPreparePool(poolFactory);
         balancer.setServers(servers);
      }

      if (configuration.pingOnStartup())
         pingServers();
   }

   private void pingServers() {
      GenericKeyedObjectPool<SocketAddress, TcpTransport> pool = getConnectionPool();
      for (SocketAddress addr : servers) {
         try {
            // Go through all statically configured nodes and force a
            // connection to be established and a ping message to be sent.
            pool.returnObject(addr, pool.borrowObject(addr));
         } catch (Exception e) {
            // Ping's objective is to retrieve a potentially newer
            // version of the Hot Rod cluster topology, so ignore
            // exceptions from nodes that might not be up any more.
            if (log.isTraceEnabled())
               log.tracef(e, "Ignoring exception pinging configured servers %s to establish a connection",
                     servers);
         }
      }
   }

   /**
    * This will makes sure that, when the evictor thread kicks in the minIdle is set. We don't want to do this is the caller's thread,
    * as this is the user.
    */
   private void createAndPreparePool(PropsKeyedObjectPoolFactory<SocketAddress, TcpTransport> poolFactory) {
      connectionPool = (GenericKeyedObjectPool<SocketAddress, TcpTransport>)
            poolFactory.createPool();
      for (SocketAddress addr: servers) {
         connectionPool.preparePool(addr, false);
      }
   }

   @Override
   public void destroy() {
      synchronized (lock) {
         connectionPool.clear();
         try {
            connectionPool.close();
         } catch (Exception e) {
            log.warn("Exception while shutting down the connection pool.", e);
         }
      }
   }

   @Override
   public void updateHashFunction(Map<SocketAddress, Set<Integer>> servers2Hash, int numKeyOwners, short hashFunctionVersion, int hashSpace) {
       synchronized (lock) {
         ConsistentHash hash = hashFactory.newConsistentHash(hashFunctionVersion);
         if (hash == null) {
            log.noHasHFunctionConfigured(hashFunctionVersion);
         } else {
            hash.init(servers2Hash, numKeyOwners, hashSpace);
         }
         consistentHash = hash;
      }
   }

   @Override
   public Transport getTransport(Set<SocketAddress> failedServers) {
      SocketAddress server;
      synchronized (lock) {
         server = balancer.nextServer(failedServers);
      }
      return borrowTransportFromPool(server);
   }

   @Override
   public Transport getTransport(byte[] key, Set<SocketAddress> failedServers) {
      SocketAddress server;
      synchronized (lock) {
         if (consistentHash != null) {
            server = consistentHash.getServer(key);
            if (log.isTraceEnabled()) {
               log.tracef("Using consistent hash for determining the server: " + server);
            }
         } else {
            server = balancer.nextServer(failedServers);
            if (log.isTraceEnabled()) {
               log.tracef("Using the balancer for determining the server: %s", server);
            }
         }
      }
      return borrowTransportFromPool(server);
   }

   @Override
   public void releaseTransport(Transport transport) {
      // The invalidateObject()/returnObject() calls could take a long time, so we hold the lock only until we get the connection pool reference
      KeyedObjectPool<SocketAddress, TcpTransport> pool = getConnectionPool();
      TcpTransport tcpTransport = (TcpTransport) transport;
      if (!tcpTransport.isValid()) {
         try {
            if (log.isTraceEnabled()) {
               log.tracef("Dropping connection as it is no longer valid: %s", tcpTransport);
            }
            pool.invalidateObject(tcpTransport.getServerAddress(), tcpTransport);
         } catch (Exception e) {
            log.couldNoInvalidateConnection(tcpTransport, e);
         }
      } else {
         try {
            pool.returnObject(tcpTransport.getServerAddress(), tcpTransport);
         } catch (Exception e) {
            log.couldNotReleaseConnection(tcpTransport, e);
         } finally {
            logConnectionInfo(tcpTransport.getServerAddress());
         }
      }
   }

   @Override
   public void invalidateTransport(SocketAddress serverAddress, Transport transport) {
      KeyedObjectPool<SocketAddress, TcpTransport> pool = getConnectionPool();
      try {
         // Transport could be null, in which case all connections
         // to the server address will be invalidated
         pool.invalidateObject(serverAddress, (TcpTransport) transport);
      } catch (Exception e) {
         log.unableToInvalidateTransport(serverAddress);
      }
   }

   @Override
   public void updateServers(Collection<SocketAddress> newServers) {
      synchronized (lock) {
         Set<SocketAddress> addedServers = new HashSet<SocketAddress>(newServers);
         addedServers.removeAll(servers);
         Set<SocketAddress> failedServers = new HashSet<SocketAddress>(servers);
         failedServers.removeAll(newServers);
         if (log.isTraceEnabled()) {
            log.tracef("Current list: %s", servers);
            log.tracef("New list: %s", newServers);
            log.tracef("Added servers: %s", addedServers);
            log.tracef("Removed servers: %s", failedServers);
         }
         if (failedServers.isEmpty() && newServers.isEmpty()) {
            log.debug("Same list of servers, not changing the pool");
            return;
         }

         //1. first add new servers. For servers that went down, the returned transport will fail for now
         for (SocketAddress server : addedServers) {
            log.newServerAdded(server);
            try {
               connectionPool.addObject(server);
            } catch (Exception e) {
               log.failedAddingNewServer(server, e);
            }
         }

         //2. now set the server list to the active list of servers. All the active servers (potentially together with some
         // failed servers) are in the pool now. But after this, the pool won't be asked for connections to failed servers,
         // as the balancer will only know about the active servers
         balancer.setServers(newServers);


         //3. Now just remove failed servers
         for (SocketAddress server : failedServers) {
            log.removingServer(server);
            connectionPool.clear(server);
         }

         servers = Collections.unmodifiableList(new ArrayList(newServers));
      }
   }

   public Collection<SocketAddress> getServers() {
      synchronized (lock) {
         return servers;
      }
   }

   private void logConnectionInfo(SocketAddress server) {
      if (log.isTraceEnabled()) {
         KeyedObjectPool<SocketAddress, TcpTransport> pool = getConnectionPool();
         log.tracef("For server %s: active = %d; idle = %d",
               server, pool.getNumActive(server), pool.getNumIdle(server));
      }
   }

   private Transport borrowTransportFromPool(SocketAddress server) {
      // The borrowObject() call could take a long time, so we hold the lock only until we get the connection pool reference
      KeyedObjectPool<SocketAddress, TcpTransport> pool = getConnectionPool();
      try {
         return pool.borrowObject(server);
      } catch (Exception e) {
         String message = "Could not fetch transport";
         log.couldNotFetchTransport(e);
         throw new TransportException(message, e, server);
      } finally {
         logConnectionInfo(server);
      }
   }

   /**
    * Note that the returned <code>ConsistentHash</code> may not be thread-safe.
    */
   public ConsistentHash getConsistentHash() {
      synchronized (lock) {
         return consistentHash;
      }
   }

   @Override
   public ConsistentHashFactory getConsistentHashFactory() {
      return hashFactory;
   }

   @Override
   public boolean isTcpNoDelay() {
      return tcpNoDelay;
   }

   @Override
   public int getMaxRetries() {
      if (Thread.currentThread().isInterrupted()) {
         return -1;
      }
      return maxRetries;
   }

   @Override
   public int getSoTimeout() {
      return soTimeout;
   }

   @Override
   public int getConnectTimeout() {
      return connectTimeout;
   }

   @Override
   public SSLContext getSSLContext() {
      return sslContext;
   }

   /**
    * Note that the returned <code>RequestBalancingStrategy</code> may not be thread-safe.
    */
   public RequestBalancingStrategy getBalancer() {
      synchronized (lock) {
         return balancer;
      }
   }

   public GenericKeyedObjectPool<SocketAddress, TcpTransport> getConnectionPool() {
      synchronized (lock) {
         return connectionPool;
      }
   }
}
