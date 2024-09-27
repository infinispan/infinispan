package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.client.hotrod.impl.Util.await;
import static org.infinispan.client.hotrod.impl.Util.wrapBytes;
import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ClusterConfiguration;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ServerConfiguration;
import org.infinispan.client.hotrod.configuration.SslConfiguration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.MarshallerRegistry;
import org.infinispan.client.hotrod.impl.TopologyInfo;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.CodecHolder;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.client.hotrod.impl.topology.ClusterInfo;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelPool.ChannelEventType;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.io.FileWatcher;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.stat.CounterTracker;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.SslContextFactory;

import com.github.benmanes.caffeine.cache.Caffeine;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.RoundRobinDnsAddressResolverGroup;
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

   private final ReadWriteLock lock = new ReentrantReadWriteLock();
   private final ConcurrentMap<SocketAddress, ChannelPool> channelPoolMap = new ConcurrentHashMap<>();
   private final Function<SocketAddress, ChannelPool> newPool = this::newPool;
   private EventLoopGroup eventLoopGroup;
   private ExecutorService executorService;
   private OperationsFactory operationsFactory;
   private int maxRetries;
   private Marshaller marshaller;
   private ClientListenerNotifier listenerNotifier;
   @GuardedBy("lock")
   private volatile TopologyInfo topologyInfo;

   private List<ClusterInfo> clusters;

   private MarshallerRegistry marshallerRegistry;
   private final LongAdder totalRetries = new LongAdder();

   @GuardedBy("lock")
   private CompletableFuture<Void> clusterSwitchStage;
   // Servers for which the last connection attempt failed and which have no established connections
   @GuardedBy("lock")
   private final Set<SocketAddress> failedServers;
   private final Configuration configuration;
   private final CodecHolder codecHolder;
   private AddressResolverGroup<?> dnsResolver;
   private SslContext sslContext;
   private FileWatcher watcher;
   private CounterTracker totalRetriesMetric = CounterTracker.NO_OP;

   public ChannelFactory(Configuration configuration, CodecHolder codecHolder) {
      this.configuration = configuration;
      this.codecHolder = codecHolder;

      this.failedServers = configuration.clientIntelligence() == ClientIntelligence.BASIC && configuration.basicFailedServerTimeout() > 0 ?
            Collections.newSetFromMap(Caffeine.newBuilder()
                  .expireAfterWrite(configuration.basicFailedServerTimeout(), TimeUnit.MILLISECONDS)
                  .<SocketAddress, Boolean>build().asMap())
            : new HashSet<>();
   }

   public void start(Marshaller marshaller, ExecutorService executorService,
                     ClientListenerNotifier listenerNotifier, MarshallerRegistry marshallerRegistry) {
      this.marshallerRegistry = marshallerRegistry;
      lock.writeLock().lock();
      try {
         this.marshaller = marshaller;
         this.executorService = executorService;
         this.listenerNotifier = listenerNotifier;
         int asyncThreads = maxAsyncThreads(executorService, configuration);
         // static field with default is private in MultithreadEventLoopGroup
         int eventLoopThreads =
               Integer.getInteger("io.netty.eventLoopThreads", ProcessorInfo.availableProcessors() * 2);
         // Note that each event loop opens a selector which counts
         int maxExecutors = Math.min(asyncThreads, eventLoopThreads);
         this.eventLoopGroup = configuration.transportFactory().createEventLoopGroup(maxExecutors, executorService);
         DnsNameResolverBuilder builder = new DnsNameResolverBuilder()
               .channelType(configuration.transportFactory().datagramChannelClass())
               .ttl(configuration.dnsResolverMinTTL(), configuration.dnsResolverMaxTTL())
               .negativeTtl(configuration.dnsResolverNegativeTTL());
         this.dnsResolver = new RoundRobinDnsAddressResolverGroup(builder);
         this.sslContext = initSslContext();

         List<InetSocketAddress> initialServers = new ArrayList<>();
         for (ServerConfiguration server : configuration.servers()) {
            initialServers.add(InetSocketAddress.createUnresolved(server.host(), server.port()));
         }
         ClusterInfo mainCluster = new ClusterInfo(DEFAULT_CLUSTER_NAME, initialServers, configuration.clientIntelligence(), configuration.security().ssl().sniHostName());
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
               ClientIntelligence intelligence = clusterConfiguration.getClientIntelligence() != null ?
                     clusterConfiguration.getClientIntelligence() :
                     configuration.clientIntelligence();

               String sniHostName = clusterConfiguration.sniHostName() != null ? clusterConfiguration.sniHostName() : configuration.security().ssl().sniHostName();
               ClusterInfo alternateCluster =
                     new ClusterInfo(clusterConfiguration.getClusterName(), alternateServers, intelligence, sniHostName);
               log.debugf("Add secondary cluster: %s", alternateCluster);
               clustersDefinitions.add(alternateCluster);
            }
            clustersDefinitions.add(mainCluster);
         }
         clusters = List.copyOf(clustersDefinitions);
         topologyInfo = new TopologyInfo(configuration, mainCluster);
         operationsFactory = new OperationsFactory(this, listenerNotifier, configuration, 0);
         maxRetries = configuration.maxRetries();

         WrappedByteArray defaultCacheName = wrapBytes(RemoteCacheManager.cacheNameBytes());
         topologyInfo.getOrCreateCacheInfo(defaultCacheName);
         registerMetrics();
      } finally {
         lock.writeLock().unlock();
      }
      pingServersIgnoreException();
   }

   private void registerMetrics() {
      var metricsRegistry = configuration.metricRegistry();
      totalRetriesMetric = metricsRegistry.createCounter("connection.pool.retries", "The total number of retries", Map.of(), null);
      metricsRegistry.createGauge("connection.pool.size", "The total number of connections", () -> getNumActive() + getNumIdle(), Map.of(), null);
   }

   private SslContext initSslContext() {
      SslConfiguration ssl = configuration.security().ssl();
      if (!ssl.enabled()) {
         return null;
      } else if (ssl.sslContext() == null) {
         this.watcher = new FileWatcher();
         SslContextBuilder builder = SslContextBuilder.forClient();
         try {
            if (ssl.keyStoreFileName() != null) {
               builder.keyManager(new SslContextFactory()
                     .keyStoreFileName(ssl.keyStoreFileName())
                     .keyStoreType(ssl.keyStoreType())
                     .keyStorePassword(ssl.keyStorePassword())
                     .keyAlias(ssl.keyAlias())
                     .classLoader(configuration.classLoader())
                     .provider(ssl.provider())
                     .watcher(watcher)
                     .build().keyManager());
            }
            if (ssl.trustStoreFileName() != null) {
               if ("pem".equalsIgnoreCase(ssl.trustStoreType())) {
                  builder.trustManager(new File(ssl.trustStoreFileName()));
               } else {
                  builder.trustManager(new SslContextFactory()
                        .trustStoreFileName(ssl.trustStoreFileName())
                        .trustStoreType(ssl.trustStoreType())
                        .trustStorePassword(ssl.trustStorePassword())
                        .classLoader(configuration.classLoader())
                        .provider(ssl.provider())
                        .watcher(watcher)
                        .build()
                        .trustManager());
               }
            }
            if (ssl.trustStorePath() != null) {
               builder.trustManager(new File(ssl.trustStorePath()));
            }
            if (ssl.protocol() != null) {
               builder.protocols(ssl.protocol());
            }
            if (ssl.ciphers() != null) {
               builder.ciphers(ssl.ciphers());
            }
            if (ssl.provider() != null) {
               Provider provider = SslContextFactory.findProvider(ssl.provider(), SslContext.class.getSimpleName(), "TLS");
               builder.sslContextProvider(provider);
            }
            return builder.build();
         } catch (Exception e) {
            throw new CacheConfigurationException(e);
         }
      } else {
         return new JdkSslContext(ssl.sslContext(), true, null, IdentityCipherSuiteFilter.INSTANCE,
               null, ClientAuth.NONE, null, false);
      }
   }

   public Codec getNegotiatedCodec() {
      return codecHolder.getCodec();
   }

   public void setNegotiatedCodec(Codec negotiatedCodec) {
      codecHolder.setCodec(negotiatedCodec);
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
            .resolver(dnsResolver)
            .remoteAddress(address)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.connectionTimeout())
            .option(ChannelOption.SO_KEEPALIVE, configuration.tcpKeepAlive())
            .option(ChannelOption.TCP_NODELAY, configuration.tcpNoDelay())
            .option(ChannelOption.SO_RCVBUF, 1024576);
      ChannelInitializer channelInitializer = createChannelInitializer(address, bootstrap);
      bootstrap.handler(channelInitializer);
      ChannelPool pool = createChannelPool(bootstrap, channelInitializer, address);
      channelInitializer.setChannelPool(pool);
      return pool;
   }

   public ChannelInitializer createChannelInitializer(SocketAddress address, Bootstrap bootstrap) {
      return new ChannelInitializer(bootstrap, address, operationsFactory, configuration, this, topologyInfo.getCluster(), sslContext);
   }

   protected ChannelPool createChannelPool(Bootstrap bootstrap, ChannelInitializer channelInitializer, SocketAddress address) {
      int maxConnections = configuration.connectionPool().maxActive();
      if (maxConnections < 0) {
         maxConnections = Integer.MAX_VALUE;
      }
      return new ChannelPool(bootstrap.config().group().next(), address, channelInitializer,
            configuration.connectionPool().exhaustedAction(), this::onConnectionEvent,
            configuration.connectionPool().maxWait(), maxConnections,
            configuration.connectionPool().maxPendingRequests(),
            configuration.metricRegistry());
   }

   protected final OperationsFactory getOperationsFactory() {
      return operationsFactory;
   }

   private void pingServersIgnoreException() {
      Collection<InetSocketAddress> servers = topologyInfo.getAllServers();
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
         if (watcher != null) {
            watcher.stop();
         }
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

   public <T extends ChannelOperation> T fetchChannelAndInvoke(Set<SocketAddress> failedServers, byte[] cacheName,
                                                               T operation) {
      return fetchChannelAndInvoke(failedServers, cacheName, operation, true);
   }

   private <T extends ChannelOperation> T fetchChannelAndInvoke(Set<SocketAddress> failedServers, byte[] cacheName,
                                                                T operation, boolean checkServer) {
      SocketAddress server;
      // Need the write lock because FailoverRequestBalancingStrategy is not thread-safe
      lock.writeLock().lock();
      try {
         if (failedServers != null) {
            CompletableFuture<Void> switchStage = this.clusterSwitchStage;
            if (switchStage != null) {
               switchStage.whenComplete((__, t) -> fetchChannelAndInvoke(failedServers, cacheName, operation));
               return operation;
            }
         }

         CacheInfo cacheInfo = topologyInfo.getCacheInfo(wrapBytes(cacheName));
         FailoverRequestBalancingStrategy balancer = cacheInfo.getBalancer();
         server = balancer.nextServer(failedServers);
      } finally {
         lock.writeLock().unlock();
      }
      return checkServer
            ? fetchChannelAndInvoke(server, cacheName, operation)
            : fetchChannelAndInvoke(server, operation);
   }

   // Package-private for testing purposes.
   <T extends ChannelOperation> T fetchChannelAndInvoke(SocketAddress preferred, byte[] cacheName, T operation) {
      boolean suspect;
      lock.readLock().lock();
      try {
         suspect = failedServers.contains(preferred);
      } finally {
         lock.readLock().unlock();
      }
      if (suspect) {
         if (log.isTraceEnabled()) log.tracef("Server %s is suspected, trying another for %s", preferred, operation);
         return fetchChannelAndInvoke(failedServers, cacheName, operation, false);
      }
      return fetchChannelAndInvoke(preferred, operation);
   }

   public <T extends ChannelOperation> T fetchChannelAndInvoke(SocketAddress server, T operation) {
      ChannelPool pool = channelPoolMap.computeIfAbsent(server, newPool);
      pool.acquire(operation);
      return operation;
   }

   private void closeChannelPools(Set<? extends SocketAddress> servers) {
      for (SocketAddress server : servers) {
         HOTROD.removingServer(server);
         ChannelPool pool = channelPoolMap.remove(server);
         if (pool != null) {
            pool.close();
         }
      }

      // We don't care if the server is failed any more
      lock.writeLock().lock();
      try {
         this.failedServers.removeAll(servers);
      } finally {
         lock.writeLock().unlock();
      }
   }

   public SocketAddress getHashAwareServer(Object key, byte[] cacheName) {
      CacheInfo cacheInfo = topologyInfo.getCacheInfo(wrapBytes(cacheName));
      if (cacheInfo != null && cacheInfo.getConsistentHash() != null) {
         return cacheInfo.getConsistentHash().getServer(key);
      }

      return null;
   }

   public <T extends ChannelOperation> T fetchChannelAndInvoke(Object key, Set<SocketAddress> failedServers,
                                                               byte[] cacheName, T operation) {
      CacheInfo cacheInfo = topologyInfo.getCacheInfo(wrapBytes(cacheName));
      if (cacheInfo != null && cacheInfo.getConsistentHash() != null) {
         SocketAddress server = cacheInfo.getConsistentHash().getServer(operation.routingObject(key));
         if (server != null && (failedServers == null || !failedServers.contains(server))) {
            return fetchChannelAndInvoke(server, cacheName, operation);
         }
      }
      return fetchChannelAndInvoke(failedServers, cacheName, operation);
   }

   public void releaseChannel(Channel channel) {
      // Due to ISPN-7955 we need to keep addresses unresolved. However resolved and unresolved addresses
      // are not deemed equal, and that breaks the comparison in channelPool - had we used channel.remoteAddress()
      // we'd create another pool for this resolved address. Therefore we need to find out appropriate pool this
      // channel belongs using the attribute.
      ChannelRecord record = ChannelRecord.of(channel);
      record.release(channel);
   }

   public void receiveTopology(byte[] cacheName, int responseTopologyAge, int responseTopologyId,
                               InetSocketAddress[] addresses, SocketAddress[][] segmentOwners,
                               short hashFunctionVersion) {
      WrappedByteArray wrappedCacheName = wrapBytes(cacheName);
      lock.writeLock().lock();
      try {
         CacheInfo cacheInfo = topologyInfo.getCacheInfo(wrappedCacheName);
         assert cacheInfo != null : "The cache info must exist before receiving a topology update";

         // Only accept the update if it's from the current age and the topology id is greater than the current one
         // Relies on TopologyInfo.switchCluster() to update the topologyAge for caches first
         if (responseTopologyAge == cacheInfo.getTopologyAge() && responseTopologyId != cacheInfo.getTopologyId()) {
            List<InetSocketAddress> addressList = Arrays.asList(addresses);
            HOTROD.newTopology(responseTopologyId, responseTopologyAge, addresses.length, addressList);
            CacheInfo newCacheInfo;
            if (hashFunctionVersion >= 0) {
               SegmentConsistentHash consistentHash =
                     createConsistentHash(segmentOwners, hashFunctionVersion, cacheInfo.getCacheName());
               newCacheInfo = cacheInfo.withNewHash(responseTopologyAge, responseTopologyId, addressList,
                     consistentHash, segmentOwners.length);
            } else {
               newCacheInfo = cacheInfo.withNewServers(responseTopologyAge, responseTopologyId, addressList);
            }
            updateCacheInfo(wrappedCacheName, newCacheInfo);
         } else {
            if (log.isTraceEnabled())
               log.tracef("[%s] Ignoring outdated topology: topology id = %s, topology age = %s, servers = %s",
                     cacheInfo.getCacheName(), responseTopologyId, responseTopologyAge,
                     Arrays.toString(addresses));
         }
      } finally {
         lock.writeLock().unlock();
      }
   }

   private SegmentConsistentHash createConsistentHash(SocketAddress[][] segmentOwners, short hashFunctionVersion,
                                                      String cacheNameString) {
      if (log.isTraceEnabled()) {
         if (hashFunctionVersion == 0)
            log.tracef("[%s] Not using a consistent hash function (hash function version == 0).",
                  cacheNameString);
         else
            log.tracef("[%s] Updating client hash function with %s number of segments",
                  cacheNameString, segmentOwners.length);
      }
      return topologyInfo.createConsistentHash(segmentOwners.length, hashFunctionVersion, segmentOwners);
   }

   @GuardedBy("lock")
   protected void updateCacheInfo(WrappedBytes cacheName, CacheInfo newCacheInfo) {
      List<InetSocketAddress> newServers = newCacheInfo.getServers();
      CacheInfo oldCacheInfo = topologyInfo.getCacheInfo(cacheName);
      List<InetSocketAddress> oldServers = oldCacheInfo.getServers();
      Set<SocketAddress> addedServers = new HashSet<>(newServers);
      oldServers.forEach(addedServers::remove);
      Set<SocketAddress> removedServers = new HashSet<>(oldServers);
      newServers.forEach(removedServers::remove);
      if (log.isTraceEnabled()) {
         String cacheNameString = newCacheInfo.getCacheName();
         log.tracef("[%s] Current list: %s", cacheNameString, oldServers);
         log.tracef("[%s] New list: %s", cacheNameString, newServers);
         log.tracef("[%s] Added servers: %s", cacheNameString, addedServers);
         log.tracef("[%s] Removed servers: %s", cacheNameString, removedServers);
      }

      // First add new servers. For servers that went down, the returned transport will fail for now
      for (SocketAddress server : addedServers) {
         HOTROD.newServerAdded(server);
         fetchChannelAndInvoke(server, operationsFactory.newPingOperation(true));
      }

      // Then update the server list for new operations
      topologyInfo.updateCacheInfo(cacheName, oldCacheInfo, newCacheInfo);

      // TODO Do not close a server pool until the server has been removed from all cache infos
      // And finally remove the failed servers
      closeChannelPools(removedServers);

      if (!removedServers.isEmpty()) {
         listenerNotifier.failoverListeners(removedServers);
      }
   }

   public Collection<InetSocketAddress> getServers() {
      lock.readLock().lock();
      try {
         return topologyInfo.getAllServers();
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

   public AtomicReference<ClientTopology> createTopologyId(byte[] cacheName) {
      lock.writeLock().lock();
      try {
         return topologyInfo.getOrCreateCacheInfo(wrapBytes(cacheName)).getClientTopologyRef();
      } finally {
         lock.writeLock().unlock();
      }
   }

   public int getTopologyId(byte[] cacheName) {
      return topologyInfo.getCacheInfo(wrapBytes(cacheName)).getTopologyId();
   }

   public void onConnectionEvent(ChannelPool pool, ChannelEventType type) {
      boolean allInitialServersFailed;
      lock.writeLock().lock();
      try {
         // TODO Replace with a simpler "pool healthy/unhealthy" event?
         if (type == ChannelEventType.CONNECTED) {
            failedServers.remove(pool.getAddress());
            return;
         } else if (type == ChannelEventType.CONNECT_FAILED) {
            if (pool.getConnected() == 0) {
               failedServers.add(pool.getAddress());
            }
         } else {
            // Nothing to do
            return;
         }

         if (log.isTraceEnabled())
            log.tracef("Connection attempt failed, we now have %d servers with no established connections: %s",
                  failedServers.size(), failedServers);
         allInitialServersFailed = failedServers.containsAll(topologyInfo.getCluster().getInitialServers());
         if (!allInitialServersFailed || clusters.isEmpty()) {
            resetCachesWithFailedServers();
         }
      } finally {
         lock.writeLock().unlock();
      }

      if (allInitialServersFailed && !clusters.isEmpty()) {
         trySwitchCluster();
      }
   }

   private void trySwitchCluster() {
      int ageBeforeSwitch;
      ClusterInfo cluster;
      lock.writeLock().lock();
      try {
         ageBeforeSwitch = topologyInfo.getTopologyAge();
         cluster = topologyInfo.getCluster();
         if (clusterSwitchStage != null) {
            if (log.isTraceEnabled())
               log.tracef("Cluster switch is already in progress for topology age %d", ageBeforeSwitch);
            return;
         }

         clusterSwitchStage = new CompletableFuture<>();
      } finally {
         lock.writeLock().unlock();
      }

      checkServersAlive(cluster.getInitialServers())
            .thenCompose(alive -> {
               if (alive) {
                  // The live check removed the server from failedServers when it established a connection
                  if (log.isTraceEnabled()) log.tracef("Cluster %s is still alive, not switching", cluster);
                  return CompletableFuture.completedFuture(null);
               }

               if (log.isTraceEnabled())
                  log.tracef("Trying to switch cluster away from '%s'", cluster.getName());
               return findLiveCluster(cluster, ageBeforeSwitch);
            })
            .thenAccept(newCluster -> {
               if (newCluster != null) {
                  automaticSwitchToCluster(newCluster, cluster, ageBeforeSwitch);
               }
            })
            .whenComplete((__, t) -> completeClusterSwitch());
   }

   @GuardedBy("lock")
   private void resetCachesWithFailedServers() {
      List<WrappedBytes> failedCaches = new ArrayList<>();
      List<String> nameStrings = new ArrayList<>();
      topologyInfo.forEachCache((cacheNameBytes, cacheInfo) -> {
         List<InetSocketAddress> cacheServers = cacheInfo.getServers();
         boolean currentServersHaveFailed = failedServers.containsAll(cacheServers);
         boolean canReset = !cacheServers.equals(topologyInfo.getCluster().getInitialServers());
         if (currentServersHaveFailed && canReset) {
            failedCaches.add(cacheNameBytes);
            nameStrings.add(cacheInfo.getCacheName());
         }
      });
      if (!failedCaches.isEmpty()) {
         HOTROD.revertCacheToInitialServerList(nameStrings);
         for (WrappedBytes cacheNameBytes : failedCaches) {
            topologyInfo.reset(cacheNameBytes);
         }
      }
   }

   private void completeClusterSwitch() {
      CompletableFuture<Void> localStage;
      lock.writeLock().lock();
      try {
         localStage = this.clusterSwitchStage;
         this.clusterSwitchStage = null;
      } finally {
         lock.writeLock().unlock();
      }

      // An automatic cluster switch could be cancelled by a manual switch,
      // and a manual cluster switch would not have a stage to begin with
      if (localStage != null) {
         localStage.complete(null);
      }
   }

   private CompletionStage<ClusterInfo> findLiveCluster(ClusterInfo failedCluster, int ageBeforeSwitch) {
      List<ClusterInfo> candidateClusters = new ArrayList<>();
      for (ClusterInfo cluster : clusters) {
         String clusterName = cluster.getName();
         if (!clusterName.equals(failedCluster.getName()))
            candidateClusters.add(cluster);
      }

      Iterator<ClusterInfo> clusterIterator = candidateClusters.iterator();
      return findLiveCluster0(false, null, clusterIterator, ageBeforeSwitch);
   }

   private CompletionStage<ClusterInfo> findLiveCluster0(boolean alive, ClusterInfo testedCluster,
                                                         Iterator<ClusterInfo> clusterIterator, int ageBeforeSwitch) {
      lock.writeLock().lock();
      try {
         if (clusterSwitchStage == null || topologyInfo.getTopologyAge() != ageBeforeSwitch) {
            log.debugf("Cluster switch already completed by another thread, bailing out");
            return CompletableFuture.completedFuture(null);
         }
      } finally {
         lock.writeLock().unlock();
      }

      if (alive) return CompletableFuture.completedFuture(testedCluster);

      if (!clusterIterator.hasNext()) {
         log.debugf("All cluster addresses viewed and none worked: %s", clusters);
         return CompletableFuture.completedFuture(null);
      }
      ClusterInfo nextCluster = clusterIterator.next();
      return checkServersAlive(nextCluster.getInitialServers())
            .thenCompose(aliveNext -> findLiveCluster0(aliveNext, nextCluster, clusterIterator, ageBeforeSwitch));
   }

   private CompletionStage<Boolean> checkServersAlive(Collection<InetSocketAddress> servers) {
      if (servers.isEmpty())
         return CompletableFuture.completedFuture(false);

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

   private void automaticSwitchToCluster(ClusterInfo newCluster, ClusterInfo failedCluster, int ageBeforeSwitch) {
      lock.writeLock().lock();
      try {
         if (clusterSwitchStage == null || topologyInfo.getTopologyAge() != ageBeforeSwitch) {
            log.debugf("Cluster switch already completed by another thread, bailing out");
            return;
         }

         topologyInfo.switchCluster(newCluster);
      } finally {
         lock.writeLock().unlock();
      }

      if (!newCluster.getName().equals(DEFAULT_CLUSTER_NAME))
         HOTROD.switchedToCluster(newCluster.getName());
      else
         HOTROD.switchedBackToMainCluster();
   }

   /**
    * Switch to an alternate cluster (or from an alternate cluster back to the main cluster).
    *
    * <p>Overrides any automatic cluster switch in progress, which may be useful
    * when the automatic switch takes too long.</p>
    */
   public boolean manualSwitchToCluster(String clusterName) {
      if (clusters.isEmpty()) {
         log.debugf("No alternative clusters configured, so can't switch cluster");
         return false;
      }

      ClusterInfo cluster = findCluster(clusterName);
      if (cluster == null) {
         log.debugf("Cluster named %s does not exist in the configuration", clusterName);
         return false;
      }

      lock.writeLock().lock();
      boolean shouldComplete = false;
      try {
         if (clusterSwitchStage != null) {
            log.debugf("Another cluster switch is already in progress, overriding it");
            shouldComplete = true;
         }
         log.debugf("Switching to cluster %s, servers: %s", clusterName, cluster.getInitialServers());
         topologyInfo.switchCluster(cluster);
      } finally {
         lock.writeLock().unlock();
      }

      if (!clusterName.equals(DEFAULT_CLUSTER_NAME))
         HOTROD.manuallySwitchedToCluster(clusterName);
      else
         HOTROD.manuallySwitchedBackToMainCluster();

      if (shouldComplete) {
         completeClusterSwitch();
      }
      return true;
   }

   public Marshaller getMarshaller() {
      return marshaller;
   }

   public String getCurrentClusterName() {
      return topologyInfo.getCluster().getName();
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

   public Set<SocketAddress> getFailedServers() {
      return failedServers;
   }

   public long getRetries() {
      return totalRetries.longValue();
   }

   public void incrementRetryCount() {
      totalRetries.increment();
      totalRetriesMetric.increment();
   }

   public ClientIntelligence getClientIntelligence() {
      lock.readLock().lock();
      try {
         return topologyInfo.getCluster().getIntelligence();
      } finally {
         lock.readLock().unlock();
      }
   }
}
