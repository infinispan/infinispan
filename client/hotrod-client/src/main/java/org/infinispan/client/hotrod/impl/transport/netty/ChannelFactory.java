package org.infinispan.client.hotrod.impl.transport.netty;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.client.hotrod.impl.Util.await;
import static org.infinispan.client.hotrod.impl.Util.wrapBytes;
import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClusterConfiguration;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ServerConfiguration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.MarshallerRegistry;
import org.infinispan.client.hotrod.impl.TopologyInfo;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.client.hotrod.impl.topology.ClusterInfo;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.ProcessorInfo;

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
   private static final CompletableFuture<ClusterSwitchStatus> NOT_SWITCHED_FUTURE = completedFuture(ClusterSwitchStatus.NOT_SWITCHED);
   private static final CompletableFuture<ClusterSwitchStatus> IN_PROGRESS_FUTURE = completedFuture(ClusterSwitchStatus.IN_PROGRESS);
   private static final CompletableFuture<ClusterSwitchStatus> SWITCHED_FUTURE = completedFuture(ClusterSwitchStatus.SWITCHED);

   private final ReadWriteLock lock = new ReentrantReadWriteLock();
   private final ConcurrentMap<SocketAddress, ChannelPool> channelPoolMap = new ConcurrentHashMap<>();
   private final Function<SocketAddress, ChannelPool> newPool = this::newPool;
   private EventLoopGroup eventLoopGroup;
   private ExecutorService executorService;
   private OperationsFactory operationsFactory;
   private Configuration configuration;
   private int maxRetries;
   private Marshaller marshaller;
   private ClientListenerNotifier listenerNotifier;
   @GuardedBy("lock")
   private volatile TopologyInfo topologyInfo;

   private List<ClusterInfo> clusters;

   private MarshallerRegistry marshallerRegistry;
   private final LongAdder totalRetries = new LongAdder();

   public void start(Codec codec, Configuration configuration, Marshaller marshaller, ExecutorService executorService,
                     ClientListenerNotifier listenerNotifier, MarshallerRegistry marshallerRegistry) {
      this.marshallerRegistry = marshallerRegistry;
      lock.writeLock().lock();
      try {
         this.marshaller = marshaller;
         this.configuration = configuration;
         this.executorService = executorService;
         this.listenerNotifier = listenerNotifier;
         int asyncThreads = maxAsyncThreads(executorService, configuration);
         // static field with default is private in MultithreadEventLoopGroup
         int eventLoopThreads =
               SecurityActions.getIntProperty("io.netty.eventLoopThreads", ProcessorInfo.availableProcessors() * 2);
         // Note that each event loop opens a selector which counts
         int maxExecutors = Math.min(asyncThreads, eventLoopThreads);
         this.eventLoopGroup = configuration.transportFactory().createEventLoopGroup(maxExecutors, executorService);

         List<InetSocketAddress> initialServers = new ArrayList<>();
         for (ServerConfiguration server : configuration.servers()) {
            initialServers.add(InetSocketAddress.createUnresolved(server.host(), server.port()));
         }
         ClusterInfo mainCluster = new ClusterInfo(DEFAULT_CLUSTER_NAME, initialServers);
         List<ClusterInfo> clustersDefinitions = new ArrayList<>();
         if (log.isDebugEnabled()) {
            log.debugf("Statically configured servers: %s", initialServers);
            log.debugf("Tcp no delay = %b; client socket timeout = %d ms; connect timeout = %d ms",
                       configuration.tcpNoDelay(), configuration.socketTimeout(), configuration.connectionTimeout());
         }

         if (!configuration.clusters().isEmpty()) {
            for (ClusterConfiguration clusterConfiguration : configuration.clusters()) {
               List<InetSocketAddress> alternateServers = new ArrayList<>();
               for (ServerConfiguration server : clusterConfiguration.getCluster()) {
                  alternateServers.add(InetSocketAddress.createUnresolved(server.host(), server.port()));
               }
               ClusterInfo alternateCluster =
                     new ClusterInfo(clusterConfiguration.getClusterName(), alternateServers);
               log.debugf("Add secondary cluster: %s", alternateCluster);
               clustersDefinitions.add(alternateCluster);
            }
            clustersDefinitions.add(mainCluster);
         }
         clusters = Immutables.immutableListCopy(clustersDefinitions);
         topologyInfo = new TopologyInfo(configuration, mainCluster);
         operationsFactory = new OperationsFactory(this, codec, listenerNotifier, configuration);
         maxRetries = configuration.maxRetries();

         WrappedByteArray defaultCacheName = wrapBytes(RemoteCacheManager.cacheNameBytes());
         topologyInfo.createCacheInfo(defaultCacheName);
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

   public MarshallerRegistry getMarshallerRegistry() {
      return marshallerRegistry;
   }

   private ChannelPool newPool(SocketAddress address) {
      log.debugf("Creating new channel pool for %s", address);
      Bootstrap bootstrap = new Bootstrap()
            .group(eventLoopGroup)
            .channel(configuration.transportFactory().socketChannelClass())
            .remoteAddress(address)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.connectionTimeout())
            .option(ChannelOption.SO_KEEPALIVE, configuration.tcpKeepAlive())
            .option(ChannelOption.TCP_NODELAY, configuration.tcpNoDelay())
            .option(ChannelOption.SO_RCVBUF, 1024576);
      int maxConnections = configuration.connectionPool().maxActive();
      if (maxConnections < 0) {
         maxConnections = Integer.MAX_VALUE;
      }
      ChannelInitializer channelInitializer = new ChannelInitializer(bootstrap, address, operationsFactory, configuration, this);
      bootstrap.handler(channelInitializer);
      ChannelPool pool = new ChannelPool(bootstrap.config().group().next(), address, channelInitializer, configuration.connectionPool().exhaustedAction(),
            configuration.connectionPool().maxWait(), maxConnections, configuration.connectionPool().maxPendingRequests());
      channelInitializer.setChannelPool(pool);
      return pool;
   }

   private void pingServersIgnoreException() {
      Collection<InetSocketAddress> servers = topologyInfo.getCurrentServers();
      for (SocketAddress addr : servers) {
         // Go through all statically configured nodes and force a
         // connection to be established and a ping message to be sent.
         try {
            await(fetchChannelAndInvoke(addr, operationsFactory.newPingOperation(true)));
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

   public Map<SocketAddress, Set<Integer>> getPrimarySegmentsByAddress(byte[] cacheName) {
      lock.readLock().lock();
      try {
         return topologyInfo.getPrimarySegmentsByServer(cacheName);
      } finally {
         lock.readLock().unlock();
      }
   }

   public void updateConsistentHash(SocketAddress[][] segmentOwners, int numSegments, short hashFunctionVersion,
                                    byte[] cacheName, int topologyId, List<InetSocketAddress> servers) {
      lock.writeLock().lock();
      try {
         WrappedByteArray wrappedCacheName = wrapBytes(cacheName);
         CacheInfo oldCacheInfo = topologyInfo.getCacheInfo(wrappedCacheName);
         assert oldCacheInfo != null : "The cache info must exist before receiving a topology update";
         SegmentConsistentHash consistentHash =
               topologyInfo.createConsistentHash(numSegments, hashFunctionVersion, segmentOwners);
         CacheInfo newCacheInfo = oldCacheInfo.withNewHash(topologyId, servers, consistentHash, numSegments);
         updateCacheInfo(wrappedCacheName, newCacheInfo, false);
         newCacheInfo.getTopologyIdRef().set(topologyId);
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
         CacheInfo cacheInfo = topologyInfo.getCacheInfo(wrapBytes(cacheName));
         if (failedServers != null && failedServers.containsAll(cacheInfo.getServers())) {
            log.switchToInitialServerList();
            reset(wrapBytes(cacheName));
            closeChannelPools(failedServers);
            failedServers.clear();
         }
         FailoverRequestBalancingStrategy balancer = cacheInfo.getBalancer();
         server = balancer.nextServer(failedServers);
      } finally {
         lock.writeLock().unlock();
      }
      if (log.isTraceEnabled())
         log.tracef("[%s] Using the balancer for determining the server: %s", new String(cacheName), server);

      return server;
   }

   private void closeChannelPools(Set<? extends SocketAddress> failedServers) {
      for (SocketAddress failedServer : failedServers) {
         HOTROD.removingServer(failedServer);
         ChannelPool pool = channelPoolMap.remove(failedServer);
         if (pool != null) {
            pool.close();
         }
      }
   }

   public SocketAddress getSocketAddress(Object key, byte[] cacheName) {
      return topologyInfo.getHashAwareServer(key, cacheName);
   }

   public <T extends ChannelOperation> T fetchChannelAndInvoke(Object key, Set<SocketAddress> failedServers, byte[] cacheName, T operation) {
      SocketAddress server = topologyInfo.getHashAwareServer(key, cacheName);
      if (server == null || (failedServers != null && failedServers.contains(server))) {
         server = getNextServer(failedServers, cacheName);
      }
      return fetchChannelAndInvoke(server, operation);
   }

   public void releaseChannel(Channel channel) {
      // Due to ISPN-7955 we need to keep addresses unresolved. However resolved and unresolved addresses
      // are not deemed equal, and that breaks the comparison in channelPool - had we used channel.remoteAddress()
      // we'd create another pool for this resolved address. Therefore we need to find out appropriate pool this
      // channel belongs using the attribute.
      ChannelRecord record = ChannelRecord.of(channel);
      record.release(channel);
   }

   public void updateServers(List<InetSocketAddress> newServers, byte[] cacheName, int topologyId, boolean quiet) {
      lock.writeLock().lock();
      try {
         WrappedByteArray wrappedCacheName = wrapBytes(cacheName);
         CacheInfo oldCacheInfo = topologyInfo.getCacheInfo(wrappedCacheName);
         CacheInfo newCacheInfo = oldCacheInfo.withNewServers(topologyId, newServers);
         updateCacheInfo(wrappedCacheName, newCacheInfo, quiet);
         newCacheInfo.getTopologyIdRef().set(topologyId);
      } finally {
         lock.writeLock().unlock();
      }
   }

   @GuardedBy("lock")
   protected CacheInfo updateCacheInfo(WrappedBytes cacheName, CacheInfo newCacheInfo, boolean quiet) {
      List<InetSocketAddress> newServers = newCacheInfo.getServers();
      CacheInfo oldCacheInfo = topologyInfo.getCacheInfo(cacheName);
      List<InetSocketAddress> oldServers = oldCacheInfo.getServers();
      Set<SocketAddress> addedServers = new HashSet<>(newServers);
      addedServers.removeAll(oldServers);
      Set<SocketAddress> failedServers = new HashSet<>(oldServers);
      failedServers.removeAll(newServers);
      if (log.isTraceEnabled()) {
         String cacheNameString = cacheName == null ? "<default>" : new String(cacheName.getBytes());
         log.tracef("[%s] Current list: %s", cacheNameString, oldServers);
         log.tracef("[%s] New list: %s", cacheNameString, newServers);
         log.tracef("[%s] Added servers: %s", cacheNameString, addedServers);
         log.tracef("[%s] Removed servers: %s", cacheNameString, failedServers);
      }

      // First add new servers. For servers that went down, the returned transport will fail for now
      for (SocketAddress server : addedServers) {
         HOTROD.newServerAdded(server);
         fetchChannelAndInvoke(server, new ReleaseChannelOperation(quiet));
      }

      // Then update the server list for new operations
      topologyInfo.updateCacheInfo(cacheName, oldCacheInfo, newCacheInfo);

      // And finally remove the failed servers
      closeChannelPools(failedServers);

      if (!failedServers.isEmpty()) {
         listenerNotifier.failoverListeners(failedServers);
      }

      return oldCacheInfo;
   }

   public Collection<InetSocketAddress> getServers() {
      lock.readLock().lock();
      try {
         return topologyInfo.getCurrentServers();
      } finally {
         lock.readLock().unlock();
      }
   }

   public Collection<InetSocketAddress> getServers(byte[] cacheName) {
      lock.readLock().lock();
      try {
         return topologyInfo.getServers(wrapBytes(cacheName));
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
         return topologyInfo.getCacheInfo(wrapBytes(cacheName)).getConsistentHash();
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
      return maxRetries;
   }

   private void reset(WrappedBytes cacheName) {
      lock.writeLock().lock();
      try {
         // Switch to the initial server list of the current cluster
         ClusterInfo cluster = topologyInfo.getCurrentCluster();
         topologyInfo.switchCluster(cluster, cacheName);
      } finally {
         lock.writeLock().unlock();
      }
   }

   public AtomicInteger createTopologyId(byte[] cacheName) {
      return topologyInfo.getOrCreateCacheInfo(wrapBytes(cacheName)).getTopologyIdRef();
   }

   public int getTopologyId(byte[] cacheName) {
      return topologyInfo.getCacheInfo(wrapBytes(cacheName)).getTopologyId();
   }

   public CompletableFuture<ClusterSwitchStatus> trySwitchCluster(String failedClusterName, byte[] cacheName) {
      lock.writeLock().lock();
      try {
         if (log.isTraceEnabled())
            log.tracef("Trying to switch cluster away from '%s'", failedClusterName);

         if (clusters.isEmpty()) {
            log.debugf("No alternative clusters configured, so can't switch cluster");
            return NOT_SWITCHED_FUTURE;
         }

         String currentClusterName = topologyInfo.getCurrentCluster().getName();
         if (!isSwitchedClusterNotAvailable(failedClusterName, currentClusterName)) {
            log.debugf("Cluster already switched from failed cluster `%s` to `%s`, try again",
                  failedClusterName, currentClusterName);
            return IN_PROGRESS_FUTURE;
         }

         // Switch cluster if there has not been a topology id cluster switch reset recently,
         if (!topologyInfo.isTopologyValid(cacheName)) {
            if (log.isTraceEnabled())
               log.tracef("Cluster switch is already in progress for topology age %d", getTopologyAge());
            return IN_PROGRESS_FUTURE;
         }

         if (log.isTraceEnabled())
            log.tracef("Switching clusters, failed cluster is '%s' and current cluster name is '%s'",
                       failedClusterName, currentClusterName);

         List<ClusterInfo> candidateClusters = new ArrayList<>();
         for (ClusterInfo cluster : clusters) {
            String clusterName = cluster.getName();
            if (!clusterName.equals(failedClusterName))
               candidateClusters.add(cluster);
         }

         Iterator<ClusterInfo> clusterIterator = candidateClusters.iterator();
         if (!clusterIterator.hasNext()) {
            log.debug("No clusters to switch to.");
            return NOT_SWITCHED_FUTURE;
         }
         ClusterInfo cluster = clusterIterator.next();
         return checkServersAlive(cluster.getInitialServers())
               .thenCompose(new ClusterSwitcher(clusterIterator, cacheName, cluster));
      } finally {
         lock.writeLock().unlock();
      }
   }

   private CompletableFuture<Boolean> checkServersAlive(Collection<InetSocketAddress> servers) {
      AtomicInteger remainingResponses = new AtomicInteger(servers.size());
      CompletableFuture<Boolean> allFuture = new CompletableFuture<>();
      for (SocketAddress server : servers) {
         fetchChannelAndInvoke(server, operationsFactory.newPingOperation(true)).whenComplete((result, throwable) -> {
            if (throwable != null) {
               if (log.isTraceEnabled()) {
                  log.tracef(throwable, "Error checking whether this server is alive: %s", server);
               }
               if (remainingResponses.decrementAndGet() == 0) {
                  allFuture.complete(false);
               }
            } else {
               // One successful response is enough to be able to switch to this cluster
               log.tracef("Ping to server %s succeeded", server);
               allFuture.complete(true);
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

      ClusterInfo cluster = findCluster(clusterName);
      if (cluster != null) {
         lock.writeLock().lock();
         try {
            log.debugf("Switching to %s, servers: %s, setting topology.", clusterName, cluster.getInitialServers());
            topologyInfo.switchCluster(cluster, WrappedByteArray.EMPTY_BYTES);

            if (!clusterName.equals(DEFAULT_CLUSTER_NAME))
               HOTROD.manuallySwitchedToCluster(clusterName);
            else
               HOTROD.manuallySwitchedBackToMainCluster();

            return true;
         } finally {
            lock.writeLock().unlock();
         }
      }

      return false;
   }

   public String getCurrentClusterName() {
      return topologyInfo.getCurrentCluster().getName();
   }

   public int getTopologyAge() {
      return topologyInfo.getTopologyAge();
   }

   private ClusterInfo findCluster(String clusterName) {
      for (ClusterInfo cluster : clusters) {
         if (cluster.getName().equals(clusterName))
            return cluster;
      }
      return null;
   }

   /**
    * Note that the returned <code>RequestBalancingStrategy</code> may not be thread-safe.
    */
   public FailoverRequestBalancingStrategy getBalancer(byte[] cacheName) {
      lock.readLock().lock();
      try {
         return topologyInfo.getCacheInfo(wrapBytes(cacheName)).getBalancer();
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

   public Configuration getConfiguration() {
      return configuration;
   }

   public long getRetries() {
      return totalRetries.longValue();
   }

   public void incrementRetryCount() {
      totalRetries.increment();
   }

   public enum ClusterSwitchStatus {
      NOT_SWITCHED, SWITCHED, IN_PROGRESS
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
            return checkServersAlive(cluster.getInitialServers()).thenCompose(this);
         }
         topologyInfo.switchCluster(cluster, wrapBytes(cacheName));

         if (!cluster.getName().equals(DEFAULT_CLUSTER_NAME))
            HOTROD.switchedToCluster(cluster.getName());
         else
            HOTROD.switchedBackToMainCluster();

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
            HOTROD.failedAddingNewServer(address, cause);
         }
      }
   }
}
