package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ClusterConfiguration;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ServerConfiguration;
import org.infinispan.client.hotrod.event.impl.ClientEventDispatcher;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.RemoteIllegalLifecycleStateException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.TopologyInfo;
import org.infinispan.client.hotrod.impl.Util;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.operations.AddClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.ClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.DelegatingHotRodOperation;
import org.infinispan.client.hotrod.impl.operations.HotRodBulkOperation;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.operations.NoCachePingOperation;
import org.infinispan.client.hotrod.impl.operations.NoHotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.client.hotrod.impl.topology.ClusterInfo;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.stat.CounterTracker;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.CompletionStages;

import com.github.benmanes.caffeine.cache.Caffeine;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.DecoderException;
import io.reactivex.rxjava3.core.Completable;
import net.jcip.annotations.GuardedBy;

/**
 * This class handles dispatching various HotRodOperations to the appropriate server.
 * It also handles retries and failover operations.
 * <p>
 * Depending upon the operation it is first determined which server it should go to, it is then
 * sent to the ChannelHandler which creates a new channel if needed for the operation which then
 * hands it off to the OperationChannel which actually dispatches the command to the socket.
 * <p>
 * When an operation is dispatched it is first registered with the {@link HeaderDecoder} so
 * that its response can be processed.
 */
public class OperationDispatcher {
   private static final Log log = LogFactory.getLog(OperationDispatcher.class, Log.class);
   public static final String DEFAULT_CLUSTER_NAME = "___DEFAULT-CLUSTER___";

   private final StampedLock lock = new StampedLock();
   private final List<ClusterInfo> clusters;
   private final int maxRetries;
   // TODO: need to add to this on retries
   private final AtomicLong retryCounter = new AtomicLong();
   // The internal state of this is not thread safe
   @GuardedBy("lock")
   private final TopologyInfo topologyInfo;
   @GuardedBy("lock")
   private CompletableFuture<Void> clusterSwitchStage;
   // Servers for which the last connection attempt failed - when this matches the current topology we will
   // fail back to initial list and if all of those fail we fail over to a different cluster if present

   // This is normally null, however when a cluster switch happens this will be initialized and any currently
   // pending operations will be added to it. Then when a command is completed it will be removed from this
   // Set and set back to null again when empty
   @GuardedBy("lock")
   private Set<HotRodOperation<?>> priorAgeOperations = null;

   @GuardedBy("lock")
   private final Set<SocketAddress> connectionFailedServers;
   private final ChannelHandler channelHandler;
   private final TimeService timeService;

   private final ClientListenerNotifier clientListenerNotifier;
   private final CounterTracker totalRetriesMetric;

   public OperationDispatcher(Configuration configuration, ExecutorService executorService, TimeService timeService,
                              ClientListenerNotifier clientListenerNotifier, Consumer<ChannelPipeline> pipelineDecorator) {
      this.timeService = timeService;
      this.clientListenerNotifier = clientListenerNotifier;
      this.maxRetries = configuration.maxRetries();

      this.connectionFailedServers = configuration.serverFailedTimeout() > 0 ?
            Collections.newSetFromMap(Caffeine.newBuilder()
                  .expireAfterWrite(configuration.serverFailedTimeout(), TimeUnit.MILLISECONDS)
                  .<SocketAddress, Boolean>build().asMap())
            : ConcurrentHashMap.newKeySet();

      List<InetSocketAddress> initialServers = new ArrayList<>();
      for (ServerConfiguration server : configuration.servers()) {
         initialServers.add(InetSocketAddress.createUnresolved(server.host(), server.port()));
      }
      ClusterInfo mainCluster = new ClusterInfo(DEFAULT_CLUSTER_NAME, initialServers, configuration.clientIntelligence(),
            configuration.security().ssl().sniHostName());

      this.topologyInfo = new TopologyInfo(configuration, mainCluster);

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

      // Use the configuration sni host name
      channelHandler = new ChannelHandler(configuration, topologyInfo.getCluster().getSniHostName(), executorService,
            this, pipelineDecorator);

      topologyInfo.getOrCreateCacheInfo(HotRodConstants.DEFAULT_CACHE_NAME);
      totalRetriesMetric = configuration.metricRegistry().createCounter("connection.pool.retries", "The total number of retries", Map.of(), null);
   }

   public CacheInfo getCacheInfo(String cacheName) {
      // This method is invoked for EVERY key based cache operation, so we optimize the optimistic read mode
      long stamp = lock.tryOptimisticRead();
      CacheInfo cacheInfo = topologyInfo.getCacheInfo(cacheName);
      if (!lock.validate(stamp)) {
         stamp = lock.readLock();
         try {
            cacheInfo = topologyInfo.getCacheInfo(cacheName);
         } finally {
            lock.unlockRead(stamp);
         }
      }
      return cacheInfo;
   }

   ClusterInfo getClusterInfo() {
      long stamp = lock.readLock();
      try {
         return topologyInfo.getCluster();
      } finally {
         lock.unlockRead(stamp);
      }
   }

   public TimeService getTimeService() {
      return timeService;
   }

   public ClientListenerNotifier getClientListenerNotifier() {
      return clientListenerNotifier;
   }

   public void start() {
      Util.await(CompletionStages.performSequentially(topologyInfo.getCluster().getInitialServers().iterator(),
            sa -> channelHandler.startChannelIfNeeded(sa)
                  .exceptionally(t -> {
                     if (log.isTraceEnabled())
                        log.tracef(t, "Ignoring exception establishing a connection to initial server %s", sa);
                     return null;
                  })));
   }

   public void stop() {
      try {
         channelHandler.close();
      } catch (Exception e) {
         log.warn("Exception while shutting down the operation dispatcher.", e);
      }
   }

   public OperationChannel getHandlerForAddress(SocketAddress socketAddress) {
      return channelHandler.getChannelForAddress(socketAddress);
   }

   public <E> CompletionStage<E> execute(HotRodOperation<E> operation) {
      assert !(operation.isInstanceOf(ClientListenerOperation.class));
      return execute(operation, Set.of());
   }

   public <E, O extends HotRodOperation<E>> CompletionStage<E> executeBulk(String cacheName, HotRodBulkOperation<?, E, O> operation) {

      return operation.executeOperations(identifyOperationTarget(cacheName, connectionFailedServers),
            this::executeOnSingleAddress);
   }

   public CompletionStage<Channel> executeAddListener(ClientListenerOperation operation) {
      return executeAddListener(operation, getBalancer(operation.getCacheName()).nextServer(Set.of()));
   }

   public CompletionStage<Channel> executeAddListener(ClientListenerOperation operation, SocketAddress target) {
      // Unfortunately, we have to do this preemptively for the case when a listener has include current state
      // as that will not complete until all initial events have been received
      clientListenerNotifier.addDispatcher(ClientEventDispatcher.create(operation,
            target, () -> {/* TODO s*/}, operation.getRemoteCache()));
      // Note this is done here, to guarantee this is lambda is ALWAYS invoked in the event loop after completion
      // This allows a retry with a different address to be protected from concurrent events being raised
      operation.whenComplete((channel, t) -> {
         if (t != null) {
            log.errorf("Error encountered trying to add listener %s", operation.listener);
            clientListenerNotifier.removeClientListener(operation.listenerId);
         } else {
            SocketAddress unresolvedAddress = ChannelRecord.of(channel);
            if (unresolvedAddress != target) {
               // we have to reinstall the dispatcher with the correct address
               clientListenerNotifier.addDispatcher(ClientEventDispatcher.create(operation,
                     unresolvedAddress, () -> {/* TODO s*/}, operation.getRemoteCache()));
            }
            addListener(unresolvedAddress, operation.listenerId);
            clientListenerNotifier.startClientListener(operation.listenerId);
         }
      });

      return executeOnSingleAddress(operation, target);
   }

   private Function<Object, SocketAddress> identifyOperationTarget(String cacheName, Set<SocketAddress> failedServers) {
      CacheInfo info = getCacheInfo(cacheName);
      if (info != null && info.getConsistentHash() != null) {
         ConsistentHash ch = info.getConsistentHash();
         return ch::getServer;
      }

      FailoverRequestBalancingStrategy frbs = getBalancer(cacheName);
      return obj -> frbs.nextServer(failedServers);
   }

   private <E> CompletionStage<E> execute(HotRodOperation<E> operation, Set<SocketAddress> opFailedServers) {
      Object routingObj = operation.getRoutingObject();
      SocketAddress targetAddress = null;
      if (routingObj != null) {
         CacheInfo cacheInfo = getCacheInfo(operation.getCacheName());
         if (cacheInfo != null && cacheInfo.getConsistentHash() != null) {
            SocketAddress server = cacheInfo.getConsistentHash().getServer(routingObj);
            if (server != null && !opFailedServers.contains(server)) {
               targetAddress = server;
            }
         }
      }
      if (targetAddress == null) {
         targetAddress = getBalancer(operation.getCacheName()).nextServer(opFailedServers);
      }
      return executeOnSingleAddress(operation, targetAddress);
   }

   public <E> CompletionStage<E> executeOnSingleAddress(HotRodOperation<E> operation, SocketAddress socketAddress) {
      // We do an empty check, as contains will perform hashCode on the socketAddress creating a String object
      if (!connectionFailedServers.isEmpty() && connectionFailedServers.contains(socketAddress)) {
         log.tracef("Server %s is suspected, trying another for %s", socketAddress, operation);
         socketAddress = getBalancer(operation.getCacheName()).nextServer(connectionFailedServers);
      }
      log.tracef("Dispatching %s to %s", operation, socketAddress);
      return channelHandler.submitOperation(operation, Objects.requireNonNull(socketAddress));
   }

   public FailoverRequestBalancingStrategy getBalancer(String cacheName) {
      return topologyInfo.getOrCreateCacheInfo(cacheName).getBalancer();
   }

   public ClientIntelligence getClientIntelligence() {
      return getClusterInfo().getIntelligence();
   }

   public CacheTopologyInfo getCacheTopologyInfo(String cacheName) {
      return getCacheInfo(cacheName).getCacheTopologyInfo();
   }

   public ClientTopology getClientTopologyInfo(String cacheName) {
      return getCacheInfo(cacheName).getClientTopologyRef().get();
   }

   public Map<SocketAddress, Set<Integer>> getPrimarySegmentsByAddress(String cacheName) {
      CacheInfo cacheInfo = getCacheInfo(cacheName);
      return cacheInfo != null ? cacheInfo.getPrimarySegments() : null;
   }

   public Collection<InetSocketAddress> getServers() {
      long stamp = lock.readLock();
      try {
         return topologyInfo.getAllServers();
      } finally {
         lock.unlockRead(stamp);
      }
   }

   @GuardedBy("lock")
   private boolean fromPreviousAge(HotRodOperation<?> operation) {
      return priorAgeOperations != null && priorAgeOperations.contains(operation);
   }

   public void updateTopology(String cacheName, HotRodOperation<?> operation, int responseTopologyId,
                              InetSocketAddress[] addresses, SocketAddress[][] segmentOwners, short hashFunctionVersion) {
      long stamp = lock.writeLock();
      try {
         CacheInfo cacheInfo = topologyInfo.getCacheInfo(cacheName);
         assert cacheInfo != null : "The cache info must exist before receiving a topology update";

         // Only accept the update if it's from the current age and the topology id is greater than the current one
         // Relies on TopologyInfo.switchCluster() to update the topologyAge for caches first
         if (priorAgeOperations == null && responseTopologyId != cacheInfo.getTopologyId()) {
            List<InetSocketAddress> addressList = Arrays.asList(addresses);
            // We don't track topology ages anymore as a number
            HOTROD.newTopology(responseTopologyId, -1, addresses.length, addressList);
            CacheInfo newCacheInfo;
            if (hashFunctionVersion >= 0) {
               SegmentConsistentHash consistentHash =
                     createConsistentHash(segmentOwners, hashFunctionVersion, cacheInfo.getCacheName());
               newCacheInfo = cacheInfo.withNewHash(responseTopologyId, addressList,
                     consistentHash, segmentOwners.length);
            } else {
               newCacheInfo = cacheInfo.withNewServers(responseTopologyId, addressList);
            }
            updateCacheInfo(cacheName, newCacheInfo);
         } else {
            if (log.isTraceEnabled())
               log.tracef("[%s] Ignoring outdated topology: topology id = %s, previous topology age = %s, servers = %s",
                     cacheInfo.getCacheName(), responseTopologyId, fromPreviousAge(operation),
                     Arrays.toString(addresses));
         }
      } finally {
         lock.unlockWrite(stamp);
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
   protected void updateCacheInfo(String cacheName, CacheInfo newCacheInfo) {
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
         // We don't wait for server to connect as it will just enqueue as needed
         channelHandler.startChannelIfNeeded(server);
      }

      // Then update the server list for new operations
      topologyInfo.updateCacheInfo(cacheName, oldCacheInfo, newCacheInfo);

      // And finally remove the failed servers
      for (SocketAddress server : removedServers) {
         HOTROD.removingServer(server);
         connectionFailedServers.remove(server);
         channelHandler.closeChannel(server);
      }
   }

   private void trySwitchCluster() {
      int ageBeforeSwitch;
      ClusterInfo cluster;
      long stamp = lock.writeLock();
      try {
         ageBeforeSwitch = topologyInfo.getTopologyAge();
         cluster = topologyInfo.getCluster();
         if (clusterSwitchStage != null) {
            if (log.isTraceEnabled())
               log.trace("Cluster switch is already in progress");
            return;
         }

         clusterSwitchStage = new CompletableFuture<>();
      } finally {
         lock.unlockWrite(stamp);
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

   private CompletionStage<Boolean> checkServersAlive(Collection<InetSocketAddress> servers) {
      if (servers.isEmpty())
         return CompletableFuture.completedFuture(false);

      AtomicInteger remainingResponses = new AtomicInteger(servers.size());
      CompletableFuture<Boolean> allFuture = new CompletableFuture<>();
      for (SocketAddress server : servers) {
         executeOnSingleAddress(new NoCachePingOperation(), server).whenComplete((result, throwable) -> {
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
      long stamp = lock.writeLock();
      try {
         if (clusterSwitchStage == null || topologyInfo.getTopologyAge() != ageBeforeSwitch) {
            log.debugf("Cluster switch already completed by another thread, bailing out");
            return CompletableFuture.completedFuture(null);
         }
      } finally {
         lock.unlockWrite(stamp);
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

   private void automaticSwitchToCluster(ClusterInfo newCluster, ClusterInfo failedCluster, int ageBeforeSwitch) {
      long stamp = lock.writeLock();
      try {
         if (clusterSwitchStage == null || priorAgeOperations != null) {
            log.debugf("Cluster switch already completed by another thread, bailing out");
            return;
         }

         log.debugf("Switching to cluster %s, servers: %s",   newCluster.getName(), newCluster.getInitialServers());
         markPendingOperationsAsPriorAge();
         // Close all the failed socket channels
         for (SocketAddress socketAddress : topologyInfo.getAllServers()) {
            channelHandler.closeChannel(socketAddress);
            connectionFailedServers.remove(socketAddress);
         }
         topologyInfo.switchCluster(newCluster);
      } finally {
         lock.unlockWrite(stamp);
      }

      if (!newCluster.getName().equals(DEFAULT_CLUSTER_NAME))
         HOTROD.switchedToCluster(newCluster.getName());
      else
         HOTROD.switchedBackToMainCluster();
   }

   public boolean manualSwitchToCluster(String clusterName) {
      if (clusters.isEmpty()) {
         log.debugf("No alternative clusters configured, so can't switch cluster");
         return false;
      }

      ClusterInfo newCluster = null;
      for (ClusterInfo cluster : clusters) {
         if (cluster.getName().equals(clusterName))
            newCluster = cluster;
      }
      if (newCluster == null) {
         log.debugf("Cluster named %s does not exist in the configuration", clusterName);
         return false;
      }

      long stamp = lock.writeLock();
      boolean shouldComplete = false;
      try {
         if (clusterSwitchStage != null) {
            log.debugf("Another cluster switch is already in progress, overriding it");
            shouldComplete = true;
         }
         log.debugf("Switching to cluster %s, servers: %s", clusterName, newCluster.getInitialServers());
         markPendingOperationsAsPriorAge();
         topologyInfo.switchCluster(newCluster);
      } finally {
         lock.unlockWrite(stamp);
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

   @GuardedBy("lock")
   private void markPendingOperationsAsPriorAge() {
      Set<HotRodOperation<?>> set;
      // TODO: this has a window where if a command is about to be dispatched and then a new topology age is installed
      // that that command may not be found and flagged as from the old topology
      if (this.priorAgeOperations == null) {
         set = ConcurrentHashMap.newKeySet();
         this.priorAgeOperations = set;
      } else {
         set = this.priorAgeOperations;
      }
      var endPlaceholder = NoHotRodOperation.instance();
      set.add(endPlaceholder);
      channelHandler.pendingOperationFlowable()
            .concatMapCompletable(op -> {
               set.add(op);
               return Completable.fromCompletionStage(op.asCompletableFuture()
                     .whenComplete((___, t) -> set.remove(op)));
            })
            .subscribe(() -> {
               if (set.isEmpty()) {
                  long stamp = lock.writeLock();
                  try {
                     this.priorAgeOperations = null;
                  } finally {
                     lock.unlockWrite(stamp);
                  }
               }
            }, t -> log.fatal("Problem occurred while configuring prior age operations for cluster failover", t));
      set.remove(endPlaceholder);
      if (set.isEmpty()) {
         this.priorAgeOperations = null;
      }
   }

   private void completeClusterSwitch() {
      CompletableFuture<Void> localStage;
      long stamp = lock.writeLock();
      try {
         localStage = this.clusterSwitchStage;
         this.clusterSwitchStage = null;
      } finally {
         lock.unlockWrite(stamp);
      }

      // An automatic cluster switch could be cancelled by a manual switch,
      // and a manual cluster switch would not have a stage to begin with
      if (localStage != null) {
         localStage.complete(null);
      }
   }

   /**
    * This method is called back whenever an operation is completed either through success (null Throwable)
    * or through exception/error (non null Throwable)
    * @param op the operation that completed
    * @param messageId the id of the message when sent
    * @param channel the channel that performed the operation
    * @param returnValue the return value for the operation (may be null)
    * @param t the throwable, which if a problem was encountered will be not null, otherwise null
    * @param <E> the operation result type
    */
   public <E> void handleResponse(HotRodOperation<E> op, long messageId, Channel channel, E returnValue, Throwable t) {
      handleResponse(op, messageId, ChannelRecord.of(channel), returnValue, t);
   }

   public <E> void handleResponse(HotRodOperation<E> op, long messageId, SocketAddress unresolvedAddress, E returnValue, Throwable t) {
      if (log.isTraceEnabled()) {
         log.tracef("Completing message %d op %s with value %s or exception %s", messageId, op,
               org.infinispan.commons.util.Util.toStr(returnValue), t);
      }
      if (t != null) {
         RetryingHotRodOperation<?> retryOp = checkException(t, unresolvedAddress, op);
         if (retryOp != null) {
            if (op instanceof AddClientListenerOperation aclo) {
               logAndRetryOrFail(t, retryOp, aclo);
            } else {
               logAndRetryOrFail(t, retryOp);
            }
         }
      } else {
         op.asCompletableFuture().complete(returnValue);
      }
   }

   private void addListener(SocketAddress sa, byte[] listenerId) {
      OperationChannel operationChannel = channelHandler.getChannelForAddress(sa);
      if (operationChannel == null) {
         throw new IllegalStateException("Channel is not running for address " + sa);
      }
      HeaderDecoder headerDecoder = (HeaderDecoder) operationChannel.getChannel().pipeline().get(HeaderDecoder.NAME);
      headerDecoder.addListener(listenerId);
   }

   public void removeListener(SocketAddress sa, byte[] listenerId) {
      OperationChannel operationChannel = channelHandler.getChannelForAddress(sa);
      if (operationChannel != null) {
         HeaderDecoder headerDecoder = (HeaderDecoder) operationChannel.getChannel().pipeline().get(HeaderDecoder.NAME);
         headerDecoder.removeListener(listenerId);
      }
   }

   public SocketAddress unresolvedAddressForChannel(Channel c) {
      return ChannelRecord.of(c);
   }

   static class RetryingHotRodOperation<T> extends DelegatingHotRodOperation<T> {
      private final Set<SocketAddress> failedServers;
      private int retryCount;

      static <T> RetryingHotRodOperation<T> retryingOp(HotRodOperation<T> op) {
         if (op instanceof RetryingHotRodOperation<T> operation) {
            return operation;
         }
         return new RetryingHotRodOperation<>(op);
      }

      RetryingHotRodOperation(HotRodOperation<T> op) {
         super(op);

         this.failedServers = new HashSet<>();
      }

      void addFailedServer(SocketAddress socketAddress) {
         failedServers.add(socketAddress);
      }

      int incrementRetry() {
         return ++retryCount;
      }

      public Set<SocketAddress> getFailedServers() {
         return failedServers;
      }
   }

   private RetryingHotRodOperation<?> checkException(Throwable cause, SocketAddress unresolvedAddress, HotRodOperation<?> op) {
      while (cause instanceof DecoderException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      if (!op.supportRetry() || isServerError(cause) &&
            // Server can throw this when it is shutting down, just retry in next block
            ! (cause instanceof RemoteIllegalLifecycleStateException
                  || cause instanceof RemoteNodeSuspectException)) {
         op.asCompletableFuture().completeExceptionally(cause);
         return null;
      } else {
         if (Thread.interrupted()) {
            InterruptedException e = new InterruptedException();
            e.addSuppressed(cause);
            op.asCompletableFuture().completeExceptionally(e);
            return null;
         }
         var retrying = RetryingHotRodOperation.retryingOp(op);
         if (unresolvedAddress != null) {
            retrying.addFailedServer(unresolvedAddress);
         }
         return retrying;
      }
   }

   protected final boolean isServerError(Throwable t) {
      return t instanceof HotRodClientException && ((HotRodClientException) t).isServerError();
   }

   protected void logAndRetryOrFail(Throwable t, RetryingHotRodOperation<?> op) {
      if (canRetry(t, op)) {
         execute(op, op.getFailedServers());
      }
   }

   protected void logAndRetryOrFail(Throwable t, RetryingHotRodOperation<?> op, AddClientListenerOperation aclo) {
      if (canRetry(t, op)) {
         FailoverRequestBalancingStrategy balancer = getBalancer(op.getCacheName());
         SocketAddress sa = balancer.nextServer(op.getFailedServers());
         if (connectionFailedServers.contains(sa)) {
            sa = balancer.nextServer(op.getFailedServers());
         }
         // This will replace the prior dispatcher from the previous attempt failure
         clientListenerNotifier.addDispatcher(ClientEventDispatcher.create(aclo,
               sa, () -> {/* TODO s*/}, aclo.getRemoteCache()));
         executeOnSingleAddress(op, sa);
      }
   }

   protected boolean canRetry(Throwable t, RetryingHotRodOperation<?> op) {
      int retryAttempt = op.incrementRetry();
      if (retryAttempt <= maxRetries) {
         if (log.isTraceEnabled()) {
            log.tracef(t, "Exception encountered in %s. Retry %d out of %d", this, retryAttempt, maxRetries);
         }
         totalRetriesMetric.increment();
         retryCounter.incrementAndGet();
         op.reset();
         return true;
      } else {
         HOTROD.exceptionAndNoRetriesLeft(retryAttempt, maxRetries, t);
         op.asCompletableFuture().completeExceptionally(t);
         return false;
      }
   }

   public void handleConnectionFailure(OperationChannel operationChannel, Throwable t) {
      if (t != null) {
         boolean allInitialServersFailed;
         long stamp = lock.writeLock();
         try {
            connectionFailedServers.add(operationChannel.getAddress());
            allInitialServersFailed = connectionFailedServers.containsAll(topologyInfo.getCluster().getInitialServers());


            if (log.isTraceEnabled())
               log.tracef("Connection attempt failed, we now have %d servers with no established connections: %s",
                     connectionFailedServers.size(), connectionFailedServers);
            if (!allInitialServersFailed || clusters.isEmpty()) {
               resetCachesWithFailedServers();
            }
         } finally {
            lock.unlockWrite(stamp);
         }

         if (allInitialServersFailed && !clusters.isEmpty()) {
            trySwitchCluster();
         }

         // Channel will not be set on the operationChannel if it didn't connect properly
         handleChannelFailure(operationChannel, t);
      } else {
         SocketAddress unresolvedAddress = operationChannel.getAddress();
         ChannelRecord.set(operationChannel.getChannel(), unresolvedAddress);
         connectionFailedServers.remove(unresolvedAddress);
         log.tracef("OperationChannel connected: %s", operationChannel);
      }
   }

   @GuardedBy("lock")
   private void resetCachesWithFailedServers() {
      List<String> failedCaches = new ArrayList<>();
      topologyInfo.forEachCache((cacheNameBytes, cacheInfo) -> {
         List<InetSocketAddress> cacheServers = cacheInfo.getServers();
         boolean currentServersHaveFailed = connectionFailedServers.containsAll(cacheServers);
         boolean canReset = !cacheServers.equals(topologyInfo.getCluster().getInitialServers());
         if (currentServersHaveFailed && canReset) {
            failedCaches.add(cacheInfo.getCacheName());
         }
      });
      if (!failedCaches.isEmpty()) {
         HOTROD.revertCacheToInitialServerList(failedCaches);
         for (String cacheName : failedCaches) {
            topologyInfo.reset(cacheName);
         }
      }
   }

   public void handleChannelFailure(Channel channel, Throwable t) {
      assert channel.eventLoop().inEventLoop();
      SocketAddress unresolved = ChannelRecord.of(channel);
      OperationChannel operationChannel = channelHandler.getChannelForAddress(unresolved);
      if (operationChannel != null) {
         handleChannelFailure(operationChannel, t);
      }
   }

   private void handleChannelFailure(OperationChannel operationChannel, Throwable t) {
      Iterable<HotRodOperation<?>> ops = operationChannel.reconnect(t);
      for (HotRodOperation<?> op : ops) {
         handleResponse(op, -1, operationChannel.getAddress(), null, t);
      }
   }

   public String getCurrentClusterName() {
      return getClusterInfo().getName();
   }

   public long getRetries() {
      return retryCounter.get();
   }

   public ConsistentHash getConsistentHash(String cacheName) {
      return getCacheInfo(cacheName).getConsistentHash();
   }

   public int getTopologyId(String cacheName) {
      return getCacheInfo(cacheName).getTopologyId();
   }

   public Collection<InetSocketAddress> getServers(String cacheName) {
      long stamp = lock.readLock();
      try {
         return topologyInfo.getServers(cacheName);
      } finally {
         lock.unlockRead(stamp);
      }
   }

   public void addCacheTopologyInfoIfAbsent(String cacheName) {
      topologyInfo.getOrCreateCacheInfo(cacheName);
   }

   public Set<SocketAddress> getConnectionFailedServers() {
      return connectionFailedServers;
   }
}
