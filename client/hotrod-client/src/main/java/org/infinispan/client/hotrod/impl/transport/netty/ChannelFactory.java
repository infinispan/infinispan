package org.infinispan.client.hotrod.impl.transport.netty;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.client.hotrod.impl.Util.await;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ServerConfiguration;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.TopologyInfo;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.Util;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * Central component providing connections to remote server. Most of the code originates in TcpTransportFactory.
 *
 * @since 9.3
 */
@ThreadSafe
public class ChannelFactory {

   public static final String DEFAULT_CLUSTER_NAME = "___DEFAULT-CLUSTER___";
   private static final Log log = LogFactory.getLog(ChannelFactory.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final CompletableFuture<ClusterSwitchStatus> NOT_SWITCHED_FUTURE = completedFuture(ClusterSwitchStatus.NOT_SWITCHED);
   private static final CompletableFuture<ClusterSwitchStatus> IN_PROGRESS_FUTURE = completedFuture(ClusterSwitchStatus.IN_PROGRESS);
   private static final CompletableFuture<ClusterSwitchStatus> SWITCHED_FUTURE = completedFuture(ClusterSwitchStatus.SWITCHED);

   private final ReadWriteLock lock = new ReentrantReadWriteLock();
   private final ConcurrentMap<SocketAddress, ChannelPool> channelPoolMap = new ConcurrentHashMap<>();
   private final Function<SocketAddress, ChannelPool> newPool = this::newPool;
   private EventLoopGroup eventLoopGroup;
   private ExecutorService executorService;
   // Per cache request balancing strategy
   private Map<WrappedByteArray, FailoverRequestBalancingStrategy> balancers;
   private OperationsFactory operationsFactory;
   private Configuration configuration;
   private Collection<SocketAddress> initialServers;
   private int maxRetries;
   private Marshaller marshaller;
   private Collection<Consumer<Set<SocketAddress>>> failedServerNotifier;
   @GuardedBy("lock")
   private volatile TopologyInfo topologyInfo;

   private volatile String currentClusterName;
   private List<ClusterInfo> clusters = new ArrayList<>();
   // Topology age provides a way to avoid concurrent cluster view changes,
   // affecting a cluster switch. After a cluster switch, the topology age is
   // increased and so any old requests that might have received topology
   // updates won't be allowed to apply since they refer to older views.
   private final AtomicInteger topologyAge = new AtomicInteger(0);

   public void start(Codec codec, Configuration configuration, AtomicInteger defaultCacheTopologyId,
                     Marshaller marshaller, ExecutorService executorService,
                     Collection<Consumer<Set<SocketAddress>>> failedServerNotifier) {
      lock.writeLock().lock();
      try {
         this.marshaller = marshaller;
         this.configuration = configuration;
         this.executorService = executorService;
         this.failedServerNotifier = failedServerNotifier;
         int asyncThreads = maxAsyncThreads(executorService, configuration);
         // static field with default is private in MultithreadEventLoopGroup
         int eventLoopThreads = SecurityActions.getIntProperty("io.netty.eventLoopThreads", Runtime.getRuntime().availableProcessors() * 2);
         // Note that each event loop opens a selector which counts
         int maxExecutors = Math.min(asyncThreads, eventLoopThreads);
         this.eventLoopGroup = TransportHelper.createEventLoopGroup(maxExecutors, executorService);

         Collection<SocketAddress> servers = new ArrayList<>();
         initialServers = new ArrayList<>();
         for (ServerConfiguration server : configuration.servers()) {
            servers.add(InetSocketAddress.createUnresolved(server.host(), server.port()));
         }
         initialServers.addAll(servers);
         if (!configuration.clusters().isEmpty()) {
            configuration.clusters().forEach(cluster -> {
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
         operationsFactory = new OperationsFactory(this, codec, configuration);
         maxRetries = configuration.maxRetries();

         if (log.isDebugEnabled()) {
            log.debugf("Statically configured servers: %s", servers);
            log.debugf("Load balancer class: %s", configuration.balancingStrategyClass().getName());
            log.debugf("Tcp no delay = %b; client socket timeout = %d ms; connect timeout = %d ms",
                    configuration.tcpNoDelay(), configuration.socketTimeout(), configuration.connectionTimeout());
         }
         balancers = new HashMap<>();
         WrappedByteArray defaultCacheName = new WrappedByteArray(RemoteCacheManager.cacheNameBytes());
         balancers.put(defaultCacheName, createBalancer(defaultCacheName));
      } finally {
         lock.writeLock().unlock();
      }
      pingServersIgnoreException();
   }

   private int maxAsyncThreads(ExecutorService executorService, Configuration configuration) {
      if (executorService instanceof ThreadPoolExecutor) {
         return ((ThreadPoolExecutor) executorService).getMaximumPoolSize();
      }
      // Note: this is quite dangerous, if someone sets different executor factory and does not update this setting
      // we might deadlock
      return new ConfigurationProperties(configuration.asyncExecutorFactory().properties()).getDefaultExecutorFactoryPoolSize();
   }

   private ChannelPool newPool(SocketAddress address) {
      log.debugf("Creating new channel pool for %s", address);
      Bootstrap bootstrap = new Bootstrap()
            .group(eventLoopGroup)
            .channel(TransportHelper.socketChannel())
            .remoteAddress(address)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.connectionTimeout())
            .option(ChannelOption.SO_KEEPALIVE, configuration.tcpKeepAlive())
            .option(ChannelOption.TCP_NODELAY, configuration.tcpNoDelay())
            .option(ChannelOption.SO_RCVBUF, 1024576);
      int maxConnections = configuration.connectionPool().maxActive();
      if (maxConnections < 0) {
         maxConnections = Integer.MAX_VALUE;
      }
      ChannelInitializer channelInitializer = new ChannelInitializer(bootstrap, address, operationsFactory, configuration);
      bootstrap.handler(channelInitializer);
      ChannelPool pool = new ChannelPool(bootstrap.config().group().next(), address, channelInitializer, configuration.connectionPool().exhaustedAction(),
            configuration.connectionPool().maxWait(), maxConnections);
      channelInitializer.setChannelPool(pool);
      return pool;
   }

   private FailoverRequestBalancingStrategy createBalancer(WrappedByteArray cacheName) {
      FailoverRequestBalancingStrategy balancer;

      FailoverRequestBalancingStrategy cfgBalancerInstance = configuration.balancingStrategy();
      if (cfgBalancerInstance != null) {
         balancer = cfgBalancerInstance;
      } else {
         balancer = Util.getInstance(configuration.balancingStrategyClass());
      }
      balancer.setServers(topologyInfo.getServers(cacheName));
      return balancer;
   }

   private void pingServersIgnoreException() {
      Collection<SocketAddress> servers = topologyInfo.getServers();
      for (SocketAddress addr : servers) {
         // Go through all statically configured nodes and force a
         // connection to be established and a ping message to be sent.
         try {
            await(fetchChannelAndInvoke(addr, operationsFactory.newPingOperation(true)));
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

   public void destroy() {
      try {
         channelPoolMap.values().forEach(ChannelPool::close);
         eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).get();
         executorService.shutdownNow();
      } catch (Exception e) {
         log.warn("Exception while shutting down the connection pool.", e);
      }
   }

   public CacheTopologyInfo getCacheTopologyInfo(byte[] cacheName) {
      lock.readLock().lock();
      try {
         return topologyInfo.getCacheTopologyInfo(cacheName);
      } finally {
         lock.readLock().unlock();
      }
   }

   public void updateHashFunction(Map<SocketAddress, Set<Integer>> servers2Hash,
                                  int numKeyOwners, short hashFunctionVersion, int hashSpace,
                                  byte[] cacheName, AtomicInteger topologyId) {
      lock.writeLock().lock();
      try {
         topologyInfo.updateTopology(servers2Hash, numKeyOwners, hashFunctionVersion, hashSpace, cacheName, topologyId);
      } finally {
         lock.writeLock().unlock();
      }
   }

   public void updateHashFunction(SocketAddress[][] segmentOwners, int numSegments, short hashFunctionVersion,
                                  byte[] cacheName, AtomicInteger topologyId) {
      lock.writeLock().lock();
      try {
         topologyInfo.updateTopology(segmentOwners, numSegments, hashFunctionVersion, cacheName, topologyId);
      } finally {
         lock.writeLock().unlock();
      }
   }

   public <T extends ChannelOperation> T fetchChannelAndInvoke(Set<SocketAddress> failedServers, byte[] cacheName, T operation) {
      return fetchChannelAndInvoke(getNextServer(failedServers, cacheName), operation);
   }

   public <T extends ChannelOperation> T fetchChannelAndInvoke(SocketAddress server, T operation) {
      ChannelPool pool = channelPoolMap.computeIfAbsent(server, newPool);
      pool.acquire(operation);
      return operation;
   }

   private SocketAddress getNextServer(Set<SocketAddress> failedServers, byte[] cacheName) {
      // This needs to be synchronized from two reasons:
      // 1) balancers are not synchronized (that's TODO)
      // 2) FailoverRequestBalancingStrategy is not threadsafe - maybe we should make this thread-local
      // or un-share it somehow
      SocketAddress server;
      lock.writeLock().lock();
      try {
         FailoverRequestBalancingStrategy balancer = getOrCreateIfAbsentBalancer(cacheName);
         server = balancer.nextServer(failedServers);
      } finally {
         lock.writeLock().unlock();
      }
      if (trace)
         log.tracef("[%s] Using the balancer for determining the server: %s", new String(cacheName), server);

      return server;
   }

   @GuardedBy("lock")
   private FailoverRequestBalancingStrategy getOrCreateIfAbsentBalancer(byte[] cacheName) {
      return balancers.computeIfAbsent(new WrappedByteArray(cacheName), this::createBalancer);
   }

   public SocketAddress getSocketAddress(Object key, byte[] cacheName) {
      return topologyInfo.getHashAwareServer(key, cacheName).orElse(null);
   }

   public <T extends ChannelOperation> T fetchChannelAndInvoke(Object key, Set<SocketAddress> failedServers, byte[] cacheName, T operation) {
      Optional<SocketAddress> hashAwareServer = topologyInfo.getHashAwareServer(key, cacheName);
      if (failedServers != null) {
         hashAwareServer = hashAwareServer.filter(server -> !failedServers.contains(server));
      }
      SocketAddress server = hashAwareServer.orElseGet(() -> getNextServer(failedServers, cacheName));
      return fetchChannelAndInvoke(server, operation);
   }

   public void releaseChannel(Channel channel) {
      if (trace) {
         log.tracef("Releasing channel %s", channel);
      }
      // Due to ISPN-7955 we need to keep addresses unresolved. However resolved and unresolved addresses
      // are not deemed equal, and that breaks the comparison in channelPool - had we used channel.remoteAddress()
      // we'd create another pool for this resolved address. Therefore we need to find out appropriate pool this
      // channel belongs using the attribute.
      ChannelRecord record = ChannelRecord.of(channel);
      record.getChannelPool().release(channel, record);
   }

   public void updateServers(Collection<SocketAddress> newServers, byte[] cacheName, boolean quiet) {
      lock.writeLock().lock();
      try {
         Collection<SocketAddress> servers = updateTopologyInfo(cacheName, newServers, quiet);
         if (!servers.isEmpty()) {
            FailoverRequestBalancingStrategy balancer = getOrCreateIfAbsentBalancer(cacheName);
            balancer.setServers(servers);
         }
      } finally {
         lock.writeLock().unlock();
      }
   }

   private void updateServers(Collection<SocketAddress> newServers) {
      lock.writeLock().lock();
      try {
         Collection<SocketAddress> servers = updateTopologyInfo(null, newServers, true);
         if (!servers.isEmpty()) {
            for (FailoverRequestBalancingStrategy balancer : balancers.values())
               balancer.setServers(servers);
         }
      } finally {
         lock.writeLock().unlock();
      }
   }

   @GuardedBy("lock")
   private Collection<SocketAddress> updateTopologyInfo(byte[] cacheName, Collection<SocketAddress> newServers, boolean quiet) {
      Collection<SocketAddress> servers = topologyInfo.getServers(new WrappedByteArray(cacheName));
      Set<SocketAddress> addedServers = new HashSet<>(newServers);
      addedServers.removeAll(servers);
      Set<SocketAddress> failedServers = new HashSet<>(servers);
      failedServers.removeAll(newServers);
      if (trace) {
         String cacheNameString = new String(cacheName);
         log.tracef("[%s] Current list: %s", cacheNameString, servers);
         log.tracef("[%s] New list: %s", cacheNameString, newServers);
         log.tracef("[%s] Added servers: %s", cacheNameString, addedServers);
         log.tracef("[%s] Removed servers: %s", cacheNameString, failedServers);
      }

      if (failedServers.isEmpty() && addedServers.isEmpty()) {
         log.debug("Same list of servers, not changing the pool");
         return Collections.emptyList();
      }

      //1. first add new servers. For servers that went down, the returned transport will fail for now
      for (SocketAddress server : addedServers) {
         log.newServerAdded(server);
         fetchChannelAndInvoke(server, new ReleaseChannelOperation(quiet));
      }

      //2. Remove failed servers
      for (SocketAddress server : failedServers) {
         log.removingServer(server);
         ChannelPool pool = channelPoolMap.remove(server);
         if (pool != null) {
            pool.close();
         }
      }

      servers = Collections.unmodifiableList(new ArrayList<>(newServers));
      topologyInfo.updateServers(cacheName, servers);

      if (!failedServers.isEmpty()) {
         for (Consumer<Set<SocketAddress>> notifier : failedServerNotifier) {
            notifier.accept(failedServers);
         }
      }

      return servers;
   }

   public Collection<SocketAddress> getServers() {
      lock.readLock().lock();
      try {
         return topologyInfo.getServers();
      } finally {
         lock.readLock().unlock();
      }
   }

   /**
    * Note that the returned <code>ConsistentHash</code> may not be thread-safe.
    */
   public ConsistentHash getConsistentHash(byte[] cacheName) {
      lock.readLock().lock();
      try {
         return topologyInfo.getConsistentHash(cacheName);
      } finally {
         lock.readLock().unlock();
      }
   }

   public ConsistentHashFactory getConsistentHashFactory() {
      return topologyInfo.getConsistentHashFactory();
   }

   public boolean isTcpNoDelay() {
      return configuration.tcpNoDelay();
   }

   public boolean isTcpKeepAlive() {
      return configuration.tcpKeepAlive();
   }

   public int getMaxRetries() {
      if (Thread.currentThread().isInterrupted()) {
         return -1;
      }
      return maxRetries;
   }

   public void reset(byte[] cacheName) {
      updateServers(initialServers, cacheName, true);
      topologyInfo.setTopologyId(cacheName, HotRodConstants.DEFAULT_CACHE_TOPOLOGY);
   }

   public AtomicInteger createTopologyId(byte[] cacheName) {
      return topologyInfo.createTopologyId(cacheName, -1);
   }

   public int getTopologyId(byte[] cacheName) {
      return topologyInfo.getTopologyId(cacheName);
   }

   public CompletableFuture<ClusterSwitchStatus> trySwitchCluster(String failedClusterName, byte[] cacheName) {
      lock.writeLock().lock();
      try {
         if (trace)
            log.tracef("Trying to switch cluster away from '%s'", failedClusterName);

         if (clusters.isEmpty()) {
            log.debugf("No alternative clusters configured, so can't switch cluster");
            return NOT_SWITCHED_FUTURE;
         }

         String currentClusterName = this.currentClusterName;
         if (!isSwitchedClusterNotAvailable(failedClusterName, currentClusterName)) {
            log.debugf("Cluster already switched from failed cluster `%s` to `%s`, try again",
                    failedClusterName, currentClusterName);
            return IN_PROGRESS_FUTURE;
         }

         // Switch cluster if there has not been a topology id cluster switch reset recently,
         if (!topologyInfo.isTopologyValid(cacheName)) {
            return IN_PROGRESS_FUTURE;
         }

         if (trace)
            log.tracef("Switching clusters, failed cluster is '%s' and current cluster name is '%s'",
                    failedClusterName, currentClusterName);

         List<ClusterInfo> candidateClusters = new ArrayList<>();
         for (ClusterInfo cluster : clusters) {
            String clusterName = cluster.clusterName;
            if (!clusterName.equals(failedClusterName))
               candidateClusters.add(cluster);
         }

         Iterator<ClusterInfo> clusterIterator = candidateClusters.iterator();
         if (!clusterIterator.hasNext()) {
            log.debug("No clusters to switch to.");
            return NOT_SWITCHED_FUTURE;
         }
         ClusterInfo cluster = clusterIterator.next();
         return checkServersAlive(cluster.clusterAddresses)
               .thenCompose(new ClusterSwitcher(clusterIterator, cacheName, cluster));
      } finally {
         lock.writeLock().unlock();
      }
   }

   private CompletableFuture<Boolean> checkServersAlive(Collection<SocketAddress> servers) {
      AtomicInteger remainingResponses = new AtomicInteger(servers.size());
      CompletableFuture<Boolean> allFuture = new CompletableFuture<>();
      for (SocketAddress server : servers) {
         fetchChannelAndInvoke(server, operationsFactory.newPingOperation(true)).whenComplete((result, throwable) -> {
            if (throwable != null) {
               if (trace) {
                  log.tracef(throwable, "Error checking whether this server is alive: %s", server);
               }
               allFuture.complete(false);
            } else {
               if (remainingResponses.decrementAndGet() == 0) {
                  allFuture.complete(true);
               }
            }
         });
      }
      return allFuture;
   }

   private boolean isSwitchedClusterNotAvailable(String failedClusterName, String currentClusterName) {
      return currentClusterName.equals(failedClusterName);
   }

   public Marshaller getMarshaller() {
      return marshaller;
   }

   public boolean switchToCluster(String clusterName) {
      if (clusters.isEmpty()) {
         log.debugf("No alternative clusters configured, so can't switch cluster");
         return false;
      }

      Collection<SocketAddress> addresses = findClusterInfo(clusterName);
      if (!addresses.isEmpty()) {
         updateServers(addresses);
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

   public String getCurrentClusterName() {
      return currentClusterName;
   }

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
      lock.readLock().lock();
      try {
         return balancers.get(new WrappedByteArray(cacheName));
      } finally {
         lock.readLock().unlock();
      }
   }

   public int socketTimeout() {
      return configuration.socketTimeout();
   }

   public int getNumActive(SocketAddress address) {
      ChannelPool pool = channelPoolMap.get(address);
      return pool == null ? 0 : pool.getActive();
   }

   public int getNumIdle(SocketAddress address) {
      ChannelPool pool = channelPoolMap.get(address);
      return pool == null ? 0 : pool.getIdle();
   }

   public int getNumActive() {
      return channelPoolMap.values().stream().mapToInt(ChannelPool::getActive).sum();
   }

   public int getNumIdle() {
      return channelPoolMap.values().stream().mapToInt(ChannelPool::getIdle).sum();
   }


   public enum ClusterSwitchStatus {
      NOT_SWITCHED, SWITCHED, IN_PROGRESS
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

   private class ClusterSwitcher implements Function<Boolean, CompletionStage<ClusterSwitchStatus>> {
      private final Iterator<ClusterInfo> clusterIterator;
      private final byte[] cacheName;
      private ClusterInfo cluster;

      ClusterSwitcher(Iterator<ClusterInfo> clusterIterator, byte[] cacheName, ClusterInfo cluster) {
         this.clusterIterator = clusterIterator;
         this.cacheName = cacheName;
         this.cluster = cluster;
      }

      @Override
      public CompletionStage<ClusterSwitchStatus> apply(Boolean alive) {
         if (!alive) {
            if (!clusterIterator.hasNext()) {
               log.debugf("All cluster addresses viewed and none worked: %s", clusters);
               return NOT_SWITCHED_FUTURE;
            }
            cluster = clusterIterator.next();
            return checkServersAlive(cluster.clusterAddresses).thenCompose(this);
         }
         topologyAge.incrementAndGet();
         lock.writeLock().lock();
         try {
            Collection<SocketAddress> servers = updateTopologyInfo(cacheName, cluster.clusterAddresses, true);
            if (!servers.isEmpty()) {
               FailoverRequestBalancingStrategy balancer = getOrCreateIfAbsentBalancer(cacheName);
               balancer.setServers(servers);
            }
         } finally {
            lock.writeLock().unlock();
         }
         topologyInfo.setTopologyId(cacheName, HotRodConstants.SWITCH_CLUSTER_TOPOLOGY);
         //clustersViewed++; // Increase number of clusters viewed
         currentClusterName = cluster.clusterName;

         if (log.isInfoEnabled()) {
            if (!cluster.clusterName.equals(DEFAULT_CLUSTER_NAME))
               log.switchedToCluster(cluster.clusterName);
            else
               log.switchedBackToMainCluster();
         }

         return SWITCHED_FUTURE;
      }
   }

   private class ReleaseChannelOperation implements ChannelOperation {
      private final boolean quiet;

      private ReleaseChannelOperation(boolean quiet) {
         this.quiet = quiet;
      }

      @Override
      public void invoke(Channel channel) {
         releaseChannel(channel);
      }

      @Override
      public void cancel(SocketAddress address, Throwable cause) {
         if (!quiet) {
            log.failedAddingNewServer(address, cause);
         }
      }
   }
}
