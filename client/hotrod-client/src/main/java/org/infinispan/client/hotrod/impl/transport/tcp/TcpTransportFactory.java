package org.infinispan.client.hotrod.impl.transport.tcp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ServerConfiguration;
import org.infinispan.client.hotrod.configuration.SslConfiguration;
import org.infinispan.client.hotrod.event.ClientListenerNotifier;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.TopologyInfo;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.operations.AddClientListenerOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.commons.util.Util;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@ThreadSafe
public class TcpTransportFactory implements TransportFactory {

   private static final Log log = LogFactory.getLog(TcpTransportFactory.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   public static final String DEFAULT_CLUSTER_NAME = "___DEFAULT-CLUSTER___";

   /**
    * We need synchronization as the thread that calls {@link TransportFactory#start(org.infinispan.client.hotrod.impl.protocol.Codec,
    * org.infinispan.client.hotrod.configuration.Configuration, java.util.concurrent.atomic.AtomicInteger,
    * org.infinispan.client.hotrod.event.ClientListenerNotifier)}
    * might(and likely will) be different from the thread(s) that calls {@link TransportFactory#getTransport(Object,
    * java.util.Set, byte[])} or other methods
    */
   private final Object lock = new Object();
   // The connection pool implementation is assumed to be thread-safe, so we need to synchronize just the access to this field and not the method calls
   private GenericKeyedObjectPool<SocketAddress, TcpTransport> connectionPool;
   // Per cache request balancing strategy
   private Map<WrappedByteArray, FailoverRequestBalancingStrategy> balancers;
   private Configuration configuration;
   private Collection<SocketAddress> initialServers;
   // the primitive fields are often accessed separately from the rest so it makes sense not to require synchronization for them
   private volatile boolean tcpNoDelay;
   private volatile boolean tcpKeepAlive;
   private volatile int soTimeout;
   private volatile int connectTimeout;
   private volatile int maxRetries;
   private volatile SSLContext sslContext;
   private volatile String sniHostName;
   private volatile ClientListenerNotifier listenerNotifier;
   @GuardedBy("lock")
   private volatile TopologyInfo topologyInfo;

   private volatile String currentClusterName;
   private List<ClusterInfo> clusters = new ArrayList<>();
   // Topology age provides a way to avoid concurrent cluster view changes,
   // affecting a cluster switch. After a cluster switch, the topology age is
   // increased and so any old requests that might have received topology
   // updates won't be allowed to apply since they refer to older views.
   private final AtomicInteger topologyAge = new AtomicInteger(0);

   private final BlockingQueue<AddClientListenerOperation> disconnectedListeners =
         new LinkedBlockingQueue<>();

   @Override
   public void start(Codec codec, Configuration configuration, AtomicInteger defaultCacheTopologyId, ClientListenerNotifier listenerNotifier) {
      synchronized (lock) {
         this.listenerNotifier = listenerNotifier;
         this.configuration = configuration;
         Collection<SocketAddress> servers = new ArrayList<>();
         initialServers = new ArrayList<>();
         for (ServerConfiguration server : configuration.servers()) {
            servers.add(InetSocketAddress.createUnresolved(server.host(), server.port()));
         }
         initialServers.addAll(servers);
         if (!configuration.clusters().isEmpty()) {
            configuration.clusters().stream().forEach(cluster -> {
               Collection<SocketAddress> clusterAddresses = cluster.getCluster().stream()
                       .map(server -> InetSocketAddress.createUnresolved(server.host(), server.port()))
                       .collect(Collectors.toList());
               ClusterInfo clusterInfo = new ClusterInfo(cluster.getClusterName(), clusterAddresses);
               log.debugf("Add secondary cluster: %s", clusterInfo);
               clusters.add(clusterInfo);
            });
            clusters.add(new ClusterInfo(DEFAULT_CLUSTER_NAME, initialServers));
         }
         currentClusterName = DEFAULT_CLUSTER_NAME;
         topologyInfo = new TopologyInfo(defaultCacheTopologyId, Collections.unmodifiableCollection(servers), configuration);
         tcpNoDelay = configuration.tcpNoDelay();
         tcpKeepAlive = configuration.tcpKeepAlive();
         soTimeout = configuration.socketTimeout();
         connectTimeout = configuration.connectionTimeout();
         maxRetries = configuration.maxRetries();
         if (configuration.security().ssl().enabled()) {
            SslConfiguration ssl = configuration.security().ssl();
            if (ssl.sslContext() != null) {
               sslContext = ssl.sslContext();
            } else {
               sslContext = SslContextFactory.getContext(
                     ssl.keyStoreFileName(),
                     ssl.keyStoreType(),
                     ssl.keyStorePassword(),
                     ssl.keyStoreCertificatePassword(),
                     ssl.keyAlias(),
                     ssl.trustStoreFileName(),
                     ssl.trustStoreType(),
                     ssl.trustStorePassword(),
                     ssl.protocol(),
                     configuration.classLoader());
            }
            sniHostName = ssl.sniHostName();
         }

         if (log.isDebugEnabled()) {
            log.debugf("Statically configured servers: %s", servers);
            log.debugf("Load balancer class: %s", configuration.balancingStrategyClass().getName());
            log.debugf("Tcp no delay = %b; client socket timeout = %d ms; connect timeout = %d ms",
                    tcpNoDelay, soTimeout, connectTimeout);
         }
         TransportObjectFactory connectionFactory;
         if (configuration.security().authentication().enabled()) {
            connectionFactory = new SaslTransportObjectFactory(codec, this, defaultCacheTopologyId, configuration);
         } else {
            connectionFactory = new TransportObjectFactory(codec, this, defaultCacheTopologyId, configuration);
         }
         PropsKeyedObjectPoolFactory<SocketAddress, TcpTransport> poolFactory =
                 new PropsKeyedObjectPoolFactory<SocketAddress, TcpTransport>(
                         connectionFactory,
                         configuration.connectionPool());
         createAndPreparePool(poolFactory);
         balancers = new HashMap<>();
         addBalancer(new WrappedByteArray(RemoteCacheManager.cacheNameBytes()));

         pingServersIgnoreException();
      }
   }

   private FailoverRequestBalancingStrategy addBalancer(WrappedByteArray cacheName) {
      FailoverRequestBalancingStrategy balancer;

      FailoverRequestBalancingStrategy cfgBalancerInstance = configuration.balancingStrategy();
      if (cfgBalancerInstance != null) {
         balancer = cfgBalancerInstance;
      } else {
         balancer = Util.getInstance(configuration.balancingStrategyClass());
      }
      balancers.put(cacheName, balancer);
      balancer.setServers(topologyInfo.getServers(cacheName));
      return balancer;
   }

   private void pingServersIgnoreException() {
      GenericKeyedObjectPool<SocketAddress, TcpTransport> pool = getConnectionPool();
      Collection<SocketAddress> servers = topologyInfo.getServers();
      for (SocketAddress addr : servers) {
         try {
            // Go through all statically configured nodes and force a
            // connection to be established and a ping message to be sent.
            pool.returnObject(addr, pool.borrowObject(addr));
         } catch (Exception e) {
            // Ping's objective is to retrieve a potentially newer
            // version of the Hot Rod cluster topology, so ignore
            // exceptions from nodes that might not be up any more.
            if (trace)
               log.tracef(e, "Ignoring exception pinging configured servers %s to establish a connection",
                       servers);
         }
      }
   }

   /**
    * This will makes sure that, when the evictor thread kicks in the minIdle is set. We don't want to do this is the
    * caller's thread,
    * as this is the user.
    */
   private void createAndPreparePool(PropsKeyedObjectPoolFactory<SocketAddress, TcpTransport> poolFactory) {
      connectionPool = (GenericKeyedObjectPool<SocketAddress, TcpTransport>)
              poolFactory.createPool();
      Collection<SocketAddress> servers = topologyInfo.getServers();
      for (SocketAddress addr : servers) {
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
   public CacheTopologyInfo getCacheTopologyInfo(byte[] cacheName) {
      synchronized (lock) {
         return topologyInfo.getCacheTopologyInfo(cacheName);
      }
   }

   @Override
   public void updateHashFunction(Map<SocketAddress, Set<Integer>> servers2Hash,
                                  int numKeyOwners, short hashFunctionVersion, int hashSpace,
                                  byte[] cacheName, AtomicInteger topologyId) {
      synchronized (lock) {
         topologyInfo.updateTopology(servers2Hash, numKeyOwners, hashFunctionVersion, hashSpace, cacheName, topologyId);
      }
   }

   @Override
   public void updateHashFunction(SocketAddress[][] segmentOwners, int numSegments, short hashFunctionVersion,
                                  byte[] cacheName, AtomicInteger topologyId) {
      synchronized (lock) {
         topologyInfo.updateTopology(segmentOwners, numSegments, hashFunctionVersion, cacheName, topologyId);
      }
   }

   @Override
   public Transport getTransport(Set<SocketAddress> failedServers, byte[] cacheName) {
      SocketAddress server;
      synchronized (lock) {
         server = getNextServer(failedServers, cacheName);
      }
      return borrowTransportFromPool(server);
   }

   @GuardedBy("lock")
   private SocketAddress getNextServer(Set<SocketAddress> failedServers, byte[] cacheName) {
      FailoverRequestBalancingStrategy balancer = getOrCreateIfAbsentBalancer(cacheName);

      SocketAddress server = balancer.nextServer(failedServers);
      if (trace)
         log.tracef("Using the balancer for determining the server: %s", server);

      return server;
   }

   private FailoverRequestBalancingStrategy getOrCreateIfAbsentBalancer(byte[] cacheName) {
      WrappedByteArray key = new WrappedByteArray(cacheName);
      FailoverRequestBalancingStrategy balancer = balancers.get(key);
      if (balancer == null)
         balancer = addBalancer(key);
      return balancer;
   }

   @Override
   public Transport getAddressTransport(SocketAddress server) {
      return borrowTransportFromPool(server);
   }

   @Override
   public SocketAddress getSocketAddress(Object key, byte[] cacheName) {
      return topologyInfo.getHashAwareServer(key, cacheName).orElse(null);
   }

   public Transport getTransport(Object key, Set<SocketAddress> failedServers, byte[] cacheName) {
      SocketAddress server;
      synchronized (lock) {
         Optional<SocketAddress> hashAwareServer = topologyInfo.getHashAwareServer(key, cacheName);
         Optional<SocketAddress> filtered = hashAwareServer.filter(a -> failedServers == null || !failedServers.contains(a));
         server = filtered.orElse(getNextServer(failedServers, cacheName));
      }
      return borrowTransportFromPool(server);
   }

   @Override
   public void releaseTransport(Transport transport) {
      if (transport.isBusy()) {
         if (trace) {
            log.tracef("Not releasing transport since it is in use: %s", transport);
         }
         return;
      }
      // The invalidateObject()/returnObject() calls could take a long time, so we hold the lock only until we get the connection pool reference
      KeyedObjectPool<SocketAddress, TcpTransport> pool = getConnectionPool();
      TcpTransport tcpTransport = (TcpTransport) transport;
      if (!tcpTransport.isValid()) {
         try {
            if (trace) {
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
      transport.invalidate();
   }

   @Override
   public void updateServers(Collection<SocketAddress> newServers, byte[] cacheName, boolean quiet) {
      synchronized (lock) {
         Collection<SocketAddress> servers = updateTopologyInfo(cacheName, newServers, quiet);
         if (!servers.isEmpty()) {
            FailoverRequestBalancingStrategy balancer = getOrCreateIfAbsentBalancer(cacheName);
            balancer.setServers(servers);
         }
      }
   }

   private void updateServers(Collection<SocketAddress> newServers, boolean quiet) {
      synchronized (lock) {
         Collection<SocketAddress> servers = updateTopologyInfo(null, newServers, quiet);
         if (!servers.isEmpty()) {
            for (FailoverRequestBalancingStrategy balancer : balancers.values())
               balancer.setServers(servers);
         }
      }
   }

   @GuardedBy("lock")
   private Collection<SocketAddress> updateTopologyInfo(byte[] cacheName, Collection<SocketAddress> newServers, boolean quiet) {
      Collection<SocketAddress> servers = topologyInfo.getServers();
      Set<SocketAddress> addedServers = new HashSet<>(newServers);
      addedServers.removeAll(servers);
      Set<SocketAddress> failedServers = new HashSet<>(servers);
      failedServers.removeAll(newServers);
      if (trace) {
         log.tracef("Current list: %s", servers);
         log.tracef("New list: %s", newServers);
         log.tracef("Added servers: %s", addedServers);
         log.tracef("Removed servers: %s", failedServers);
      }

      if (failedServers.isEmpty() && addedServers.isEmpty()) {
         log.debug("Same list of servers, not changing the pool");
         return Collections.emptyList();
      }

      //1. first add new servers. For servers that went down, the returned transport will fail for now
      for (SocketAddress server : addedServers) {
         log.newServerAdded(server);
         try {
            connectionPool.addObject(server);
         } catch (Exception e) {
            if (!quiet) log.failedAddingNewServer(server, e);
         }
      }

      //2. Remove failed servers
      for (SocketAddress server : failedServers) {
         log.removingServer(server);
         connectionPool.clear(server);
      }

      servers = Collections.unmodifiableList(new ArrayList(newServers));
      topologyInfo.updateServers(cacheName, servers);

      if (!failedServers.isEmpty()) {
         listenerNotifier.failoverClientListeners(failedServers);
      }

      return servers;
   }

   public Collection<SocketAddress> getServers() {
      synchronized (lock) {
         return topologyInfo.getServers();
      }
   }

   private void logConnectionInfo(SocketAddress server) {
      if (trace) {
         KeyedObjectPool<SocketAddress, TcpTransport> pool = getConnectionPool();
         log.tracef("For server %s: active = %d; idle = %d",
                 server, pool.getNumActive(server), pool.getNumIdle(server));
      }
   }

   private Transport borrowTransportFromPool(SocketAddress server) {
      // The borrowObject() call could take a long time, so we hold the lock only until we get the connection pool reference
      KeyedObjectPool<SocketAddress, TcpTransport> pool = getConnectionPool();
      try {
         TcpTransport tcpTransport = pool.borrowObject(server);
         reconnectListenersIfNeeded();
         return tcpTransport;
      } catch (Exception e) {
         String message = "Could not fetch transport";
         log.debug(message, e);
         throw new TransportException(message, e, server);
      } finally {
         logConnectionInfo(server);
      }
   }

   private void reconnectListenersIfNeeded() {
      if (!disconnectedListeners.isEmpty()) {
         List<AddClientListenerOperation> drained = new ArrayList<>();
         disconnectedListeners.drainTo(drained);
         for (AddClientListenerOperation op : drained) {
            if (trace) {
               log.tracef("Reconnecting client listener with id %s", Util.printArray(op.listenerId));
            }
            op.execute();
         }
      }
   }

   /**
    * Note that the returned <code>ConsistentHash</code> may not be thread-safe.
    */
   @Override
   public ConsistentHash getConsistentHash(byte[] cacheName) {
      synchronized (lock) {
         return topologyInfo.getConsistentHash(cacheName);
      }
   }

   @Override
   public ConsistentHashFactory getConsistentHashFactory() {
      return topologyInfo.getConsistentHashFactory();
   }

   @Override
   public boolean isTcpNoDelay() {
      return tcpNoDelay;
   }

   @Override
   public boolean isTcpKeepAlive() {
      return tcpKeepAlive;
   }

   @Override
   public int getMaxRetries() {
      if (Thread.currentThread().isInterrupted()) {
         // Interrupted status not cleared, no need to set it again
         throw new HotRodClientException(new InterruptedException());
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

   @Override
   public String getSniHostName() {
      return sniHostName;
   }

   @Override
   public void addDisconnectedListener(AddClientListenerOperation listener) throws InterruptedException {
      disconnectedListeners.put(listener);
   }

   @Override
   public void reset(byte[] cacheName) {
      updateServers(initialServers, cacheName, true);
      topologyInfo.setTopologyId(cacheName, HotRodConstants.DEFAULT_CACHE_TOPOLOGY);
   }

   @Override
   public AtomicInteger createTopologyId(byte[] cacheName) {
      synchronized (lock) {
         return topologyInfo.createTopologyId(cacheName, -1);
      }
   }

   @Override
   public int getTopologyId(byte[] cacheName) {
      synchronized (lock) {
         return topologyInfo.getTopologyId(cacheName);
      }
   }

   @Override
   public ClusterSwitchStatus trySwitchCluster(String failedClusterName, byte[] cacheName) {
      synchronized (lock) {
         if (trace)
            log.tracef("Trying to switch cluster away from '%s'", failedClusterName);

         if (clusters.isEmpty()) {
            log.debugf("No alternative clusters configured, so can't switch cluster");
            return ClusterSwitchStatus.NOT_SWITCHED;
         }

         String currentClusterName = this.currentClusterName;
         if (!isSwitchedClusterNotAvailable(failedClusterName, currentClusterName)) {
            log.debugf("Cluster already switched from failed cluster `%s` to `%s`, try again",
                    failedClusterName, currentClusterName);
            return ClusterSwitchStatus.IN_PROGRESS;
         }

         // Switch cluster if there has not been a topology id cluster switch reset recently,
         if (topologyInfo.isTopologyValid(cacheName)) {
            if (trace)
               log.tracef("Switching clusters, failed cluster is '%s' and current cluster name is '%s'",
                       failedClusterName, currentClusterName);

            List<ClusterInfo> candidateClusters = new ArrayList<>();
            for (ClusterInfo cluster : clusters) {
               String clusterName = cluster.clusterName;
               if (!clusterName.equals(failedClusterName))
                  candidateClusters.add(cluster);
            }

            for (int i = 0; i < candidateClusters.size(); i++) {
               ClusterInfo cluster = candidateClusters.get(i % candidateClusters.size());
               boolean alive = checkServersAlive(cluster.clusterAddresses);
               if (alive) {
                  topologyAge.incrementAndGet();
                  Collection<SocketAddress> servers = updateTopologyInfo(cacheName, cluster.clusterAddresses, true);
                  if (!servers.isEmpty()) {
                     FailoverRequestBalancingStrategy balancer = getOrCreateIfAbsentBalancer(cacheName);
                     balancer.setServers(servers);
                  }

                  topologyInfo.setTopologyId(cacheName, HotRodConstants.SWITCH_CLUSTER_TOPOLOGY);
                  //clustersViewed++; // Increase number of clusters viewed
                  this.currentClusterName = cluster.clusterName;

                  if (log.isInfoEnabled()) {
                     if (!cluster.clusterName.equals(DEFAULT_CLUSTER_NAME))
                        log.switchedToCluster(cluster.clusterName);
                     else
                        log.switchedBackToMainCluster();
                  }

                  return ClusterSwitchStatus.SWITCHED;
               }
            }

            log.debugf("All cluster addresses viewed and none worked: %s", clusters);
            return ClusterSwitchStatus.NOT_SWITCHED;
         }

         return ClusterSwitchStatus.IN_PROGRESS;
      }
   }

   public boolean checkServersAlive(Collection<SocketAddress> servers) {
      for (SocketAddress server : servers) {
         try {
            connectionPool.addObject(server);
         } catch (Exception e) {
            log.tracef(e, "Error checking whether this server is alive: %s", server);
            return false;
         }
      }
      return true;
   }

   private boolean isSwitchedClusterNotAvailable(String failedClusterName, String currentClusterName) {
      return currentClusterName.equals(failedClusterName);
   }

   public enum ClusterSwitchStatus {
      NOT_SWITCHED, SWITCHED, IN_PROGRESS;
   }

   @Override
   public Marshaller getMarshaller() {
      return listenerNotifier.getMarshaller();
   }

   public boolean switchToCluster(String clusterName) {
      if (clusters.isEmpty()) {
         log.debugf("No alternative clusters configured, so can't switch cluster");
         return false;
      }

      Collection<SocketAddress> addresses = findClusterInfo(clusterName);
      if (!addresses.isEmpty()) {
         updateServers(addresses, true);
         topologyInfo.setAllTopologyIds(HotRodConstants.SWITCH_CLUSTER_TOPOLOGY);

         if (log.isInfoEnabled()) {
            if (!clusterName.equals(DEFAULT_CLUSTER_NAME))
               log.manuallySwitchedToCluster(clusterName);
            else
               log.manuallySwitchedBackToMainCluster();
         }

         return true;
      }

      return false;
   }

   @Override
   public String getCurrentClusterName() {
      return currentClusterName;
   }

   @Override
   public int getTopologyAge() {
      return topologyAge.get();
   }

   private Collection<SocketAddress> findClusterInfo(String clusterName) {
      for (ClusterInfo cluster : clusters) {
         if (cluster.clusterName.equals(clusterName))
            return cluster.clusterAddresses;
      }
      return Collections.emptyList();
   }

   /**
    * Note that the returned <code>RequestBalancingStrategy</code> may not be thread-safe.
    */
   public FailoverRequestBalancingStrategy getBalancer(byte[] cacheName) {
      synchronized (lock) {
         return balancers.get(new WrappedByteArray(cacheName));
      }
   }

   public GenericKeyedObjectPool<SocketAddress, TcpTransport> getConnectionPool() {
      synchronized (lock) {
         return connectionPool;
      }
   }

   private static final class ClusterInfo {
      final Collection<SocketAddress> clusterAddresses;
      final String clusterName;

      private ClusterInfo(String clusterName, Collection<SocketAddress> clusterAddresses) {
         this.clusterAddresses = clusterAddresses;
         this.clusterName = clusterName;
      }

      @Override
      public String toString() {
         return "ClusterInfo{" +
                 "name='" + clusterName + '\'' +
                 ", addresses=" + clusterAddresses +
                 '}';
      }
   }
}
