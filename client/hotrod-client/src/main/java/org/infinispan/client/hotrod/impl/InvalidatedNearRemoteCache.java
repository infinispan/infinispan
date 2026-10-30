package org.infinispan.client.hotrod.impl;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.event.impl.PrimaryNearCacheEventDispatcher;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.client.hotrod.impl.operations.ClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.GetWithMetadataOperation;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.near.NearCacheService;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;

import io.netty.channel.Channel;

/**
 * Near {@link org.infinispan.client.hotrod.RemoteCache} implementation that keeps a local in-memory
 * cache invalidated via server events.
 * <p>
 * When connecting to a Hot Rod 4.2+ cluster with hash distribution awareness, this cache operates in
 * <b>primary mode</b>, registering per-node listeners on each primary owner. If primary listener registration
 * fails or the server does not support protocol 4.2, it falls back to a single cluster-wide listener.
 * <p>
 * During rolling upgrades the effective cluster protocol version may change at runtime.  The cache
 * automatically downgrades to the cluster-wide listener when an older-protocol server joins and upgrades
 * back to primary mode once all servers are running Hot Rod 4.2+.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class InvalidatedNearRemoteCache<K, V> extends DelegatingRemoteCache<K, V> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final NearCacheService<K, V> nearcache;
   private final ClientStatistics clientStatistics;
   // This field is used to control updating the near cache with updating the remote server's bloom filter
   // representation. This value will be non-null when bloom filter is enabled, otherwise null. When this
   // value is even it means there is no bloom filter update being sent and the near cache can be updated
   // locally from reads. When a bloom filter is being replicated though the value will be odd and the
   // near cache cannot be updated.
   private final AtomicInteger bloomFilterUpdateVersion;
   private final CacheOperationsFactory cacheOperationsFactory;
   private volatile Channel listenerChannel;

   public static class ServerListenerState {
      private volatile boolean active;
      private volatile Channel channel;
      private final AtomicInteger bloomVersion = new AtomicInteger();
      private final AtomicBoolean registering = new AtomicBoolean();

      public boolean isActive() {
         return active;
      }

      public void setActive(boolean active) {
         this.active = active;
      }

      public Channel getChannel() {
         return channel;
      }

      public void setChannel(Channel channel) {
         this.channel = channel;
      }

      public boolean startRegistering() {
         return registering.compareAndSet(false, true);
      }

      public void stopRegistering() {
         registering.set(false);
      }

      public int getVersion() {
         return bloomVersion.get();
      }

      public int incrementVersion() {
         return bloomVersion.incrementAndGet();
      }

      public boolean incrementVersionIfEven() {
         int prev;
         do {
            prev = bloomVersion.get();
            if ((prev & 1) == 1) {
               return false;
            }
         } while (!bloomVersion.compareAndSet(prev, prev + 1));
         return true;
      }
   }

   private final ConcurrentMap<SocketAddress, ServerListenerState> serverListenerStates = new ConcurrentHashMap<>();
   private final BiConsumer<CacheInfo, CacheInfo> topologyListener = this::onTopologyUpdate;
   private final AtomicInteger lastSeenTopologyId = new AtomicInteger(-1);
   private volatile Object nearListener;
   private volatile byte[] nearListenerId;
   private volatile int nearBloomBits;
   // Permanently disables primary mode when initial setup fails irrecoverably (e.g. proxy port unreachable).
   private volatile boolean primaryModeDisabled = false;
   // Tracks whether the near cache is currently operating in primary-owner listener mode.
   private final AtomicBoolean inPrimaryMode = new AtomicBoolean(false);
   // Registered with OperationDispatcher to be notified when any channel's codec is negotiated.
   private Runnable codecChangeListener;
   // Guards concurrent calls to switchToClusterListenerMode / switchToPrimaryListenerMode.
   private final AtomicBoolean modeSwitch = new AtomicBoolean(false);

   InvalidatedNearRemoteCache(InternalRemoteCache<K, V> remoteCache, ClientStatistics clientStatistics,
                              NearCacheService<K, V> nearcache) {
      super(remoteCache);
      this.clientStatistics = clientStatistics;
      this.nearcache = nearcache;
      this.bloomFilterUpdateVersion = nearcache.getBloomFilterBits() > 0 ? new AtomicInteger() : null;
      this.cacheOperationsFactory = remoteCache.getOperationsFactory().newFactoryFor(this);
   }

   public CacheOperationsFactory getOperationsFactory() {
      return cacheOperationsFactory;
   }

   @Override
   <Key, Value> InternalRemoteCache<Key, Value> newDelegatingCache(InternalRemoteCache<Key, Value> innerCache) {
      return new InvalidatedNearRemoteCache<>(innerCache, clientStatistics, (NearCacheService<Key, Value>) nearcache);
   }

   public static <K, V> InvalidatedNearRemoteCache<K, V> delegatingNearCache(RemoteCacheImpl<K, V> remoteCache,
                                                                             NearCacheService<K, V> nearCacheService) {
      return new InvalidatedNearRemoteCache<>(remoteCache, remoteCache.clientStatistics, nearCacheService);
   }

   @Override
   public CompletableFuture<V> getAsync(Object key) {
      CompletableFuture<MetadataValue<V>> value = getWithMetadataAsync((K) key);
      return value.thenApply(v -> v != null ? v.getValue() : null);
   }

   public NearCacheService<K, V> getNearCacheService() {
      return nearcache;
   }

   /**
    * Checks whether primary near cache mode is currently enabled and supported by the connected cluster.
    *
    * @return {@code true} if running in primary mode with known segment ownership, {@code false} otherwise
    */
   public boolean isPrimaryMode() {
      return inPrimaryMode.get();
   }

   /**
    * Checks if at least one primary owner server has an active listener connected.
    */
   public boolean hasActivePrimaryListeners() {
      return !serverListenerStates.isEmpty() && serverListenerStates.values().stream().anyMatch(ServerListenerState::isActive);
   }

   /**
    * Checks if all servers currently owning primary segments have active listeners established.
    */
   public boolean hasAllPrimaryListenersActive() {
      Map<SocketAddress, Set<Integer>> primarySegments = getDispatcher().getPrimarySegmentsByAddress(getName());
      if (primarySegments == null || primarySegments.isEmpty()) {
         return false;
      }
      for (SocketAddress server : primarySegments.keySet()) {
         ServerListenerState state = getServerListenerState(server);
         if (state == null || !state.isActive()) {
            return false;
         }
      }
      return true;
   }

   /**
    * Permanently disables primary mode for this cache session and tears down any active primary listeners.
    * Called when initial setup fails irrecoverably. Use {@link #tearDownPrimaryListeners()} for the
    * rolling-upgrade downgrade path, which does not set the permanent disable flag.
    */
   private void cleanupPrimaryMode() {
      primaryModeDisabled = true;
      tearDownPrimaryListeners();
   }

   /**
    * Tears down all active primary owner listeners and resets the primary-mode tracking state,
    * without permanently disabling primary mode. Used for rolling-upgrade downgrade transitions.
    */
   private void tearDownPrimaryListeners() {
      inPrimaryMode.set(false);
      getDispatcher().removeTopologyListener(getName(), topologyListener);
      byte[] lid = this.nearListenerId;
      if (lid != null) {
         ClientListenerNotifier cln = getDispatcher().getClientListenerNotifier();
         for (Map.Entry<SocketAddress, ServerListenerState> entry : serverListenerStates.entrySet()) {
            if (entry.getValue().isActive()) {
               SocketAddress server = entry.getKey();
               try {
                  HotRodOperation<Void> op = getOperationsFactory().newRemoveClientListenerOperation(lid);
                  op.asCompletableFuture().thenAccept(___ -> cln.removeClientListener(lid, server));
                  getDispatcher().executeOnSingleAddress(op, server);
               } catch (Exception e) {
                  if (trace) {
                     log.tracef(e, "Error removing primary listener on %s during fallback", server);
                  }
               }
            }
         }
         cln.removeClientListener(lid);
         this.nearListenerId = null;
      }
      serverListenerStates.clear();
      lastSeenTopologyId.set(-1);
      nearcache.setSegmentFunction(null, 0);
   }

   /**
    * Locates the primary owner socket address for the given key (serialized) based on the current consistent hash.
    */
   public SocketAddress getPrimaryOwner(Object key) {
      ConsistentHash ch = getDispatcher().getConsistentHash(getName());
      return ch != null ? ch.getServer(keyToBytes(key)) : null;
   }

   private static void assertUnresolved(SocketAddress address) {
      assert address == null || !(address instanceof InetSocketAddress isa) || isa.isUnresolved()
            : "Address must be unresolved: " + address;
   }

   /**
    * Returns the current bloom filter version for the specified server if its listener is active and no
    * bloom filter update is currently in flight (i.e. version is even). Returns -1 if inactive or updating.
    */
   public int getVersionIfActive(SocketAddress server) {
      if (server == null) {
         return -1;
      }
      assertUnresolved(server);
      ServerListenerState state = serverListenerStates.get(server);
      if (state == null || !state.isActive() || state.getChannel() == null) {
         return -1;
      }
      int v = state.getVersion();
      return (v & 1) == 1 ? -1 : v;
   }

   /**
    * Returns the active channel for the given primary server, or {@code null} if no active listener exists.
    */
   public Channel getChannelIfActive(SocketAddress server) {
      if (server == null) {
         return null;
      }
      assertUnresolved(server);
      ServerListenerState state = serverListenerStates.get(server);
      return (state != null && state.isActive()) ? state.getChannel() : null;
   }

   /**
    * Verifies that the listener on the specified server is still active and that its bloom filter version
    * has not changed since the read was initiated.
    */
   public boolean isActiveAndVersionMatches(SocketAddress server, int expectedVersion) {
      if (server == null || expectedVersion < 0) {
         return false;
      }
      assertUnresolved(server);
      ServerListenerState state = serverListenerStates.get(server);
      return state != null && state.isActive() && state.getVersion() == expectedVersion;
   }

   public ServerListenerState getOrCreateServerListenerState(SocketAddress server) {
      assertUnresolved(server);
      return serverListenerStates.computeIfAbsent(server, k -> new ServerListenerState());
   }

   public void setServerListenerActive(SocketAddress server, boolean active) {
      setServerListenerActive(server, null, active);
   }

   public void setServerListenerActive(SocketAddress server, Channel channel, boolean active) {
      assertUnresolved(server);
      ServerListenerState state = getOrCreateServerListenerState(server);
      if (channel != null || !active) {
         state.setChannel(channel);
      }
      state.setActive(active);
   }

   public ServerListenerState getServerListenerState(SocketAddress server) {
      assertUnresolved(server);
      return serverListenerStates.get(server);
   }

   public Map<SocketAddress, ServerListenerState> getServerListenerStates() {
      return Collections.unmodifiableMap(serverListenerStates);
   }

   /**
    * Handles disconnection of a primary server listener: marks the listener state inactive and proactively
    * invalidates all near-cache segments primarily owned by the disconnected server to prevent serving stale data.
    */
   public void handlePrimaryListenerDisconnected(SocketAddress address) {
      assertUnresolved(address);
      ServerListenerState state = getServerListenerState(address);
      if (state != null) {
         state.setActive(false);
         state.setChannel(null);
         state.stopRegistering();
      }
      Map<SocketAddress, Set<Integer>> primarySegments = getDispatcher().getPrimarySegmentsByAddress(getName());
      if (primarySegments != null) {
         Set<Integer> segments = primarySegments.get(address);
         if (segments != null) {
            nearcache.invalidateSegments(segments);
         }
      }
   }

   public int getCurrentVersion() {
      if (bloomFilterUpdateVersion != null) {
         return bloomFilterUpdateVersion.get();
      }
      return 0;
   }

   @Override
   public CompletableFuture<MetadataValue<V>> getWithMetadataAsync(K key) {
      MetadataValue<V> nearValue = nearcache.get(key);
      // We rely upon the fact that a MetadataValue will never be null from a remote lookup but our placeholder will be null
      if (nearValue != null && nearValue.getValue() != null) {
         clientStatistics.incrementNearCacheHits();
         return CompletableFuture.completedFuture(nearValue);
      }

      clientStatistics.incrementNearCacheMisses();
      // Note that MetadataValueImpl does not implement equals method so we rely upon object identity in the replace below
      // We cannot cache this value as we could have 2 concurrent gets and an update and we could cache a previous one
      MetadataValue<V> calculatingPlaceholder = new MetadataValueImpl<>(-1, -1, -1, -1, -1, null);
      boolean cache = nearcache.putIfAbsent(key, calculatingPlaceholder);

      if (isPrimaryMode()) {
         SocketAddress primaryServer = getPrimaryOwner(key);
         int prevVersion = getVersionIfActive(primaryServer);
         Channel primaryChannel = getChannelIfActive(primaryServer);
         if (!cache || prevVersion < 0 || primaryChannel == null) {
            if (trace) {
               log.tracef("Unable to cache returned value for key %s (cache=%s, prevVersion=%d, primary=%s)",
                     Util.toStr(key), cache, prevVersion, primaryServer);
            }
            nearcache.remove(key, calculatingPlaceholder);
            return super.getWithMetadataAsync(key, null).toCompletableFuture()
                  .thenApply(GetWithMetadataOperation.GetWithMetadataResult::value);
         }
         return super.getWithMetadataAsync(key, primaryChannel)
               .thenApply(v -> storeInNearCacheOrRemove(key, calculatingPlaceholder, v,
                     isPrimaryResultValid(key, v, primaryServer, prevVersion)))
               .toCompletableFuture();
      } else {
         int prevVersion = getCurrentVersion();
         CompletionStage<GetWithMetadataOperation.GetWithMetadataResult<V>> remoteValue =
               super.getWithMetadataAsync(key, listenerChannel);
         if (!cache || (prevVersion & 1) == 1) {
            if (trace) {
               log.tracef("Unable to cache returned value for key %s as either has concurrent operation or during a bloom filter update",
                     Util.toStr(key));
            }
            nearcache.remove(key, calculatingPlaceholder);
            return remoteValue.toCompletableFuture()
                  .thenApply(GetWithMetadataOperation.GetWithMetadataResult::value);
         }
         return remoteValue
               .thenApply(v -> storeInNearCacheOrRemove(key, calculatingPlaceholder, v,
                     isClusterResultValid(key, v, prevVersion)))
               .toCompletableFuture();
      }
   }

   private boolean isPrimaryResultValid(K key, GetWithMetadataOperation.GetWithMetadataResult<V> v,
                                        SocketAddress primaryServer, int prevVersion) {
      if (v == null || v.value() == null) {
         return false;
      }
      SocketAddress currentPrimary = getPrimaryOwner(key);
      if (!Objects.equals(primaryServer, currentPrimary)) {
         if (trace) {
            log.tracef("Unable to cache returned value for key %s as primary owner changed from %s to %s",
                  Util.toStr(key), primaryServer, currentPrimary);
         }
         return false;
      }
      if (nearcache.getBloomFilterBits() > 0 && v.retried()) {
         if (trace) {
            log.tracef("Unable to cache returned value for key %s as operation was retried", Util.toStr(key));
         }
         return false;
      }
      if (!isActiveAndVersionMatches(primaryServer, prevVersion)) {
         if (trace) {
            log.tracef("Unable to cache returned value for key %s as listener state/version on %s changed",
                  Util.toStr(key), primaryServer);
         }
         return false;
      }
      return true;
   }

   private boolean isClusterResultValid(K key, GetWithMetadataOperation.GetWithMetadataResult<V> v, int prevVersion) {
      if (v == null || v.value() == null) {
         return false;
      }
      if (prevVersion != getCurrentVersion()) {
         if (trace) {
            log.tracef("Unable to cache returned value for key %s as operation was performed during a bloom filter update",
                  Util.toStr(key));
         }
         return false;
      }
      if (listenerChannel != null && v.retried()) {
         if (trace) {
            log.tracef("Unable to cache returned value for key %s as operation was retried", Util.toStr(key));
         }
         return false;
      }
      return true;
   }

   private MetadataValue<V> storeInNearCacheOrRemove(K key, MetadataValue<V> placeholder,
                                                     GetWithMetadataOperation.GetWithMetadataResult<V> result,
                                                     boolean valid) {
      MetadataValue<V> value = result != null ? result.value() : null;
      if (value != null && valid) {
         nearcache.replace(key, placeholder, value);
         if (value.getMaxIdle() > 0) {
            HOTROD.nearCacheMaxIdleUnsupported();
         }
      } else {
         nearcache.remove(key, placeholder);
      }
      return value;
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      if (maxIdleTime > 0)
         HOTROD.nearCacheMaxIdleUnsupported();
      CompletableFuture<V> ret = super.putAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return ret.thenApply(v -> {
         nearcache.remove(key);
         return v;
      });
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      if (maxIdleTime > 0)
         HOTROD.nearCacheMaxIdleUnsupported();
      return super.putAllAsync(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit)
            .thenRun(() -> map.keySet().forEach(nearcache::remove));
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      if (maxIdleTime > 0)
         HOTROD.nearCacheMaxIdleUnsupported();
      return invalidateNearCacheIfNeeded(delegate.hasForceReturnFlag(), key,
            super.replaceAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit)
      );
   }

   @Override
   public CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, long lifespan, TimeUnit lifespanTimeUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      if (maxIdleTime > 0)
         HOTROD.nearCacheMaxIdleUnsupported();
      return super.replaceWithVersionAsync(key, newValue, version, lifespan, lifespanTimeUnit, maxIdleTime, maxIdleTimeUnit)
            .thenApply(removed -> {
               if (removed) nearcache.remove(key);
               return removed;
            });
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      return invalidateNearCacheIfNeeded(delegate.hasForceReturnFlag(), key, super.removeAsync(key));
   }

   @Override
   public CompletableFuture<Boolean> removeWithVersionAsync(K key, long version) {
      return super.removeWithVersionAsync(key, version)
            .thenApply(removed -> {
               if (removed) nearcache.remove(key); // Eager invalidation to avoid race
               return removed;
            });
   }

   @Override
   public CompletableFuture<Void> clearAsync() {
      return super.clearAsync().thenRun(nearcache::clear);
   }

   /**
    * Invalidates the local near cache entry upon completion of a conditional or mutating operation if the
    * operation succeeded (or unconditionally when force return value flag is not set).
    */
   @SuppressWarnings("unchecked")
   CompletableFuture<V> invalidateNearCacheIfNeeded(boolean hasForceReturnValue, Object key, CompletableFuture<V> prev) {
      return prev.thenApply(v -> {
         if (!hasForceReturnValue || v != null)
            nearcache.remove((K) key);
         return v;
      });
   }

   @Override
   public void resolveStorage(MediaType key, MediaType value) {
      if (key != null && !delegate.getMarshaller().mediaType().match(key)) {
         // The server has a storage type which the cache marshaller does not handle.
         // This could lead to losing events in the bloom filter, where the client and server see the key differently.
         // To avoid this issue, the client negotiate the type when installing the listener, but it needs a proper configuration.
         Class<? extends Marshaller> marshallerClass = delegate.getMarshaller().getClass();
         log.invalidateNearDefaultMarshallerMismatch(delegate.getName(), marshallerClass, key);
         listenerChannel = nearcache.start(this);
         delegate.resolveStorage(key, value);
      } else {
         delegate.resolveStorage(key, value);
         listenerChannel = nearcache.start(this);
      }
   }

   @Override
   public void removeClientListener(Object listener) {
      if (isPrimaryMode() && Objects.equals(listener, this.nearListener)) {
         inPrimaryMode.set(false);
         getDispatcher().removeTopologyListener(getName(), topologyListener);
         byte[] lid = this.nearListenerId;
         if (lid != null) {
            ClientListenerNotifier cln = getDispatcher().getClientListenerNotifier();
            AggregateCompletionStage<Void> aggregate = CompletionStages.aggregateCompletionStage();
            for (SocketAddress server : serverListenerStates.keySet()) {
               HotRodOperation<Void> op = getOperationsFactory().newRemoveClientListenerOperation(lid);
               aggregate.dependsOn(op.asCompletableFuture()
                     .thenAccept(___ -> cln.removeClientListener(lid, server))
                     .exceptionally(t -> {
                        if (trace) log.tracef(t, "Error removing primary listener on %s", server);
                        return null;
                     }));
               getDispatcher().executeOnSingleAddress(op, server);
            }
            getDispatcher().await(aggregate.freeze());
            cln.removeClientListener(lid);
         }
         serverListenerStates.clear();
         lastSeenTopologyId.set(-1);
         this.nearListener = null;
         this.nearListenerId = null;
         return;
      }
      super.removeClientListener(listener);
   }

   @Override
   public void stop() {
      if (codecChangeListener != null) {
         getDispatcher().removeCodecChangeListener(codecChangeListener);
         codecChangeListener = null;
      }
      if (isPrimaryMode()) {
         getDispatcher().removeTopologyListener(getName(), topologyListener);
      }
      nearcache.stop(this);
      super.stop();
   }

   public void clearNearCache() {
      nearcache.clear();
   }

   // Increments the bloom filter version if it is even and returns whether it was incremented
   private boolean incrementBloomVersionIfEven() {
      int prev;
      do {
         prev = bloomFilterUpdateVersion.get();
         // Odd number means we are already sending bloom filter
         if ((prev & 1) == 1) {
            return false;
         }
      } while (!bloomFilterUpdateVersion.compareAndSet(prev, prev + 1));
      return true;
   }

   CompletionStage<Void> incrementBloomVersionUponCompletion(CompletionStage<Void> stage) {
      if (bloomFilterUpdateVersion != null) {
         return stage
               .whenComplete((ignore, t) -> bloomFilterUpdateVersion.incrementAndGet());
      }
      return stage;
   }

   @Override
   public CompletionStage<Void> updateBloomFilter() {
      if (bloomFilterUpdateVersion == null && nearcache.getBloomFilterBits() <= 0) {
         return CompletableFutures.completedNull();
      }
      if (isPrimaryMode()) {
         byte[] bloomFilterBits = nearcache.calculateBloomBits();
         List<CompletableFuture<Void>> stages = new ArrayList<>();
         CacheOperationsFactory operationsFactory = getOperationsFactory();
         for (Map.Entry<SocketAddress, ServerListenerState> entry : serverListenerStates.entrySet()) {
            ServerListenerState state = entry.getValue();
            if (state.isActive() && state.incrementVersionIfEven()) {
               HotRodOperation<Void> op = operationsFactory.newUpdateBloomFilterOperation(bloomFilterBits);
               CompletionStage<Void> stage = getDispatcher().executeOnSingleAddress(op, entry.getKey())
                     .whenComplete((ignore, t) -> state.incrementVersion());
               stages.add(stage.toCompletableFuture());
            } else if (trace && state.isActive()) {
               log.tracef("Already have a concurrent bloom filter update for server %s - skipping", entry.getKey());
            }
         }
         return CompletableFuture.allOf(stages.toArray(new CompletableFuture[0]));
      }
      // Not being able to increment the even version means we have a concurrent update - so skip
      if (!incrementBloomVersionIfEven()) {
         if (trace) {
            log.tracef("Already have a concurrent bloom filter update for listenerId(%s) - skipping",
                  Util.printArray(nearcache.getListenerId()));
         }
         return CompletableFuture.completedFuture(null);
      }
      byte[] bloomFilterBits = nearcache.calculateBloomBits();

      if (trace) {
         log.tracef("Sending bloom filter bits(%s) update to %s for listenerId(%s)",
               Util.printArray(bloomFilterBits), listenerChannel,
               Util.printArray(nearcache.getListenerId()));
      }
      CacheOperationsFactory operationsFactory = getOperationsFactory();
      HotRodOperation<Void> op = operationsFactory.newUpdateBloomFilterOperation(bloomFilterBits);
      return incrementBloomVersionUponCompletion(getDispatcher().executeOnSingleAddress(op, ChannelRecord.of(listenerChannel)));
   }

   public SocketAddress getBloomListenerAddress() {
      return ChannelRecord.of(listenerChannel);
   }

   public void setBloomListenerAddress(Channel channel) {
      this.listenerChannel = channel;
   }

   public NearCacheConfiguration getNearCacheConfiguration() {
      return nearcache.getConfig();
   }

   @Override
   public Channel addNearCacheListener(Object listener, int bloomBits) {
      this.nearListener = listener;
      this.nearBloomBits = bloomBits;
      lastSeenTopologyId.set(-1);

      // Register codec change listener once — reacts to rolling-upgrade protocol version transitions.
      if (codecChangeListener == null) {
         codecChangeListener = this::onCodecChange;
         getDispatcher().addCodecChangeListener(codecChangeListener);
      }

      // Guard with modeSwitch so that codec-change callbacks received while primary listener
      // registration is in flight (during the blocking await) do not race with this setup.
      // Use compareAndSet so that a concurrent call (e.g. from a reconnect) does not proceed
      // into setup simultaneously; the second caller simply falls back to the cluster listener.
      if (!modeSwitch.compareAndSet(false, true)) {
         Channel channel = installClusterListener(listener, bloomBits);
         return bloomBits > 0 ? channel : null;
      }
      try {
         if (canUsePrimaryMode()) {
            this.nearListenerId = ClientListenerOperation.generateListenerId();
            PrimaryNearCacheEventDispatcher nearDispatcher = PrimaryNearCacheEventDispatcher.create(
                  getOperationsFactory().newAddPrimaryNearCacheListenerOperation(listener, nearListenerId, bloomBits), this);
            getDispatcher().getClientListenerNotifier().addDispatcher(nearDispatcher);
            getDispatcher().addTopologyListener(getName(), topologyListener);
            try {
               Channel channel = registerPrimaryListeners(listener, nearListenerId, bloomBits);
               if (channel != null && hasActivePrimaryListeners()) {
                  inPrimaryMode.set(true);
                  return channel;
               }
               log.noPrimaryListenersEstablished(getName());
            } catch (Throwable t) {
               log.primaryListenerRegistrationFailed(getName(), t);
            }
            // Have to fallback to regular listener mode (permanent disable — initial setup failed)
            cleanupPrimaryMode();
         }
         Channel channel = installClusterListener(listener, bloomBits);
         return bloomBits > 0 ? channel : null;
      } finally {
         modeSwitch.set(false);
      }
   }

   /**
    * Returns {@code true} if primary-owner listener mode is eligible at this point in time: the dispatcher
    * intelligence is HASH_DISTRIBUTION_AWARE, all connected channels have negotiated Hot Rod 4.2+, primary
    * segment information is available, and primary mode has not been permanently disabled.
    */
   private boolean canUsePrimaryMode() {
      if (primaryModeDisabled) {
         return false;
      }
      if (!getDispatcher().isPrimaryNearCacheListenerSupported()) {
         return false;
      }
      Map<SocketAddress, Set<Integer>> primarySegments = getDispatcher().getPrimarySegmentsByAddress(getName());
      return primarySegments != null && !primarySegments.isEmpty();
   }

   /**
    * Registers a cluster-wide (non-primary) near cache listener and updates the tracked listener ID.
    */
   private Channel installClusterListener(Object listener, int bloomBits) {
      ClientListenerOperation op = getOperationsFactory().newAddNearCacheListenerOperation(listener, bloomBits);
      Channel channel = getDispatcher().await(getDispatcher().executeAddListener(op));
      this.nearListenerId = op.listenerId;
      nearcache.updateListenerId(op.listenerId);
      return channel;
   }

   /**
    * Registers primary near cache listeners across all servers owning primary segments in parallel.
    * Schedules background reconnection for any servers that fail initial registration.
    *
    * @return the channel of the first successfully registered primary listener, or {@code null}
    */
   private Channel registerPrimaryListeners(Object listener, byte[] listenerId, int bloomBits) {
      OperationDispatcher dispatcher = getDispatcher();
      ConsistentHash ch = dispatcher.getConsistentHash(getName());
      if (ch instanceof SegmentConsistentHash sch) {
         nearcache.setSegmentFunction(k -> sch.getSegment(keyToBytes(k)), sch.getNumSegments());
      }
      Map<SocketAddress, Set<Integer>> primarySegments = dispatcher.getPrimarySegmentsByAddress(getName());
      if (primarySegments == null || primarySegments.isEmpty()) {
         return null;
      }

      AtomicReference<Channel> firstActiveChannel = new AtomicReference<>();
      Set<SocketAddress> failedServers = ConcurrentHashMap.newKeySet();
      AggregateCompletionStage<Void> aggregate = CompletionStages.aggregateCompletionStage();

      for (SocketAddress server : primarySegments.keySet()) {
         CompletionStage<Channel> stage = registerPrimaryListenerOnServer(server, listener, listenerId, bloomBits)
               .whenComplete((channel, t) -> {
                  if (t == null && channel != null) {
                     firstActiveChannel.compareAndSet(null, channel);
                  } else {
                     failedServers.add(server);
                  }
               })
               .exceptionally(t -> null);
         aggregate.dependsOn(stage);
      }
      dispatcher.await(aggregate.freeze());

      Channel channel = firstActiveChannel.get();
      if (channel != null && hasActivePrimaryListeners()) {
         // If we had at least one server connect but some failed, retry the others
         for (SocketAddress failedServer : failedServers) {
            dispatcher.getClientListenerNotifier().scheduleReconnect(listenerId, failedServer);
         }
      }

      return channel;
   }

   /**
    * Registers a primary listener on a specific server channel. If bloom filters are enabled, pushes the
    * current local bloom filter state immediately upon connection establishment.
    */
   private CompletionStage<Channel> registerPrimaryListenerOnServer(SocketAddress server, Object listener, byte[] listenerId, int bloomBits) {
      ServerListenerState state = getOrCreateServerListenerState(server);
      if (state.isActive() || !state.startRegistering()) {
         return CompletableFuture.completedFuture(null);
      }
      ClientListenerOperation op = getOperationsFactory().newAddPrimaryNearCacheListenerOperation(listener, listenerId, bloomBits);
      return getDispatcher().executeAddListener(op, server).thenCompose(ch -> {
         if (bloomBits > 0) {
            byte[] currentBloomBits = nearcache.calculateBloomBits();
            if (currentBloomBits != null) {
               HotRodOperation<Void> updateOp = getOperationsFactory().newUpdateBloomFilterOperation(currentBloomBits);
               return getDispatcher().executeOnSingleAddress(updateOp, ChannelRecord.of(ch))
                     .thenApply(ignore -> {
                        state.setChannel(ch);
                        state.setActive(true);
                        state.stopRegistering();
                        return ch;
                     });
            }
         }
         state.setChannel(ch);
         state.setActive(true);
         state.stopRegistering();
         return CompletableFuture.completedFuture(ch);
      }).whenComplete((ch, t) -> {
         if (t != null) {
            state.setActive(false);
            state.setChannel(null);
            state.stopRegistering();
            if (trace) {
               log.tracef(t, "Failed to register primary listener on server %s", server);
            }
         }
      });
   }

   /**
    * Reacts to a codec negotiation event fired by {@code HeaderDecoder} when any channel has its protocol
    * version finalised.  This is the trigger for rolling-upgrade mode transitions:
    * <ul>
    *   <li>If we are currently in primary mode but a channel now reports a codec that does not support
    *       primary near cache listeners (e.g. a 4.1 server joined), we downgrade to the cluster-wide
    *       listener and clear the near cache.</li>
    *   <li>If we are currently in cluster-wide mode and all channels now support primary near cache
    *       listeners (e.g. the last 4.1 server was upgraded to 4.2), we switch back to primary mode
    *       and clear the near cache.</li>
    * </ul>
    */
   private void onCodecChange() {
      boolean allSupport = getDispatcher().isPrimaryNearCacheListenerSupported();
      if (inPrimaryMode.get() && !allSupport) {
         // A lower-version server has connected — downgrade.
         // Submit to the dispatcher executor to avoid blocking the Netty event loop.
         getDispatcher().getExecutorService().submit(this::switchToClusterListenerMode);
      } else if (!inPrimaryMode.get() && !primaryModeDisabled && allSupport) {
         // All servers now support primary mode — try to upgrade.
         getDispatcher().getExecutorService().submit(this::switchToPrimaryListenerMode);
      }
   }

   /**
    * Downgrades the near cache from primary-owner listener mode to a single cluster-wide listener.
    * Triggered when an older-protocol server joins the cluster during a rolling upgrade.
    * The near cache is cleared to ensure no stale entries remain from the transition window.
    */
   private void switchToClusterListenerMode() {
      if (!modeSwitch.compareAndSet(false, true)) {
         return;
      }
      try {
         if (!inPrimaryMode.get()) {
            return; // already in cluster mode, nothing to do
         }
         log.nearCacheDowngradedToClusterListener(getName());
         Object listener = this.nearListener;
         int bloomBits = this.nearBloomBits;
         // Remove primary listeners (without permanently disabling primary mode).
         tearDownPrimaryListeners();
         // Clear the near cache to avoid serving stale entries during the transition.
         nearcache.clear();
         // Install a fresh cluster-wide listener.
         if (listener != null) {
            installClusterListener(listener, bloomBits);
         }
      } finally {
         modeSwitch.set(false);
      }
   }

   /**
    * Upgrades the near cache from a cluster-wide listener to primary-owner listener mode.
    * Triggered when all servers in the cluster have been upgraded to Hot Rod 4.2+.
    * The near cache is cleared to ensure correctness after the transition window.
    */
   private void switchToPrimaryListenerMode() {
      if (!modeSwitch.compareAndSet(false, true)) {
         return;
      }
      try {
         if (inPrimaryMode.get() || primaryModeDisabled) {
            return;
         }
         // Re-check inside the guarded section: another thread may have changed things.
         if (!canUsePrimaryMode()) {
            return;
         }
         log.nearCacheUpgradedToPrimaryListeners(getName());
         Object listener = this.nearListener;
         int bloomBits = this.nearBloomBits;

         // Remove the existing cluster-wide listener via the standard path.
         if (listener != null) {
            try {
               super.removeClientListener(listener);
            } catch (Exception e) {
               if (trace) {
                  log.tracef(e, "Error removing cluster listener during upgrade to primary mode for cache %s", getName());
               }
            }
         }
         this.nearListenerId = null;
         listenerChannel = null;

         // Clear the near cache before the switch to avoid stale entries.
         nearcache.clear();

         // Install primary listeners.
         lastSeenTopologyId.set(-1);
         if (listener != null) {
            byte[] newListenerId = ClientListenerOperation.generateListenerId();
            this.nearListenerId = newListenerId;
            PrimaryNearCacheEventDispatcher nearDispatcher = PrimaryNearCacheEventDispatcher.create(
                  getOperationsFactory().newAddPrimaryNearCacheListenerOperation(listener, newListenerId, bloomBits), this);
            getDispatcher().getClientListenerNotifier().addDispatcher(nearDispatcher);
            getDispatcher().addTopologyListener(getName(), topologyListener);
            try {
               Channel channel = registerPrimaryListeners(listener, newListenerId, bloomBits);
               if (channel != null && hasActivePrimaryListeners()) {
                  inPrimaryMode.set(true);
                  listenerChannel = channel;
                  nearcache.updateListenerId(newListenerId);
                  return;
               }
            } catch (Throwable t) {
               if (trace) {
                  log.tracef(t, "Failed to register primary listeners during upgrade for cache %s", getName());
               }
            }
            // Could not establish primary listeners — stay in cluster mode but don't permanently disable.
            getDispatcher().removeTopologyListener(getName(), topologyListener);
            getDispatcher().getClientListenerNotifier().removeClientListener(newListenerId);
            // Re-install the cluster listener
            installClusterListener(listener, bloomBits);
         }
      } finally {
         modeSwitch.set(false);
      }
   }

   /**
    * Reacts to a cluster topology change:
    * <ul>
    *    <li>Filters out stale or duplicate topology updates using monotonic topology IDs.</li>
    *    <li>Computes segment ownership differences and invalidates only segments whose primary owner changed.</li>
    *    <li>Unregisters and cleans up listeners on servers that no longer own primary segments.</li>
    *    <li>Installs new primary listeners on newly appointed primary owners.</li>
    * </ul>
    */
   private void onTopologyUpdate(CacheInfo oldCacheInfo, CacheInfo newCacheInfo) {
      int newTopologyId = newCacheInfo != null ? newCacheInfo.getTopologyId() : -1;
      if (newTopologyId < 0) {
         lastSeenTopologyId.set(-1);
         return;
      }
      if (lastSeenTopologyId.getAndUpdate(cur -> Math.max(cur, newTopologyId)) >= newTopologyId) {
         if (trace) {
            log.tracef("Ignoring stale or duplicate topology update in near cache: newTopologyId=%d, lastSeenTopologyId=%d",
                  newTopologyId, lastSeenTopologyId.get());
         }
         return;
      }

      ConsistentHash oldCH = oldCacheInfo != null ? oldCacheInfo.getConsistentHash() : null;
      ConsistentHash newCH = newCacheInfo != null ? newCacheInfo.getConsistentHash() : null;

      if (newCH instanceof SegmentConsistentHash newSCH) {
         int numSegments = newSCH.getNumSegments();
         nearcache.setSegmentFunction(k -> newSCH.getSegment(keyToBytes(k)), numSegments);

         if (oldCH instanceof SegmentConsistentHash oldSCH) {
            SocketAddress[][] oldOwners = oldSCH.getSegmentOwners();
            SocketAddress[][] newOwners = newSCH.getSegmentOwners();
            for (int seg = 0; seg < numSegments; ++seg) {
               SocketAddress oldPrimary = (oldOwners != null && seg < oldOwners.length && oldOwners[seg].length > 0) ? oldOwners[seg][0] : null;
               SocketAddress newPrimary = (newOwners != null && seg < newOwners.length && newOwners[seg].length > 0) ? newOwners[seg][0] : null;
               if (!Objects.equals(oldPrimary, newPrimary)) {
                  nearcache.invalidateSegment(seg);
               }
            }
         } else {
            for (int seg = 0; seg < numSegments; ++seg) {
               nearcache.invalidateSegment(seg);
            }
         }

         // Register listeners on any new primary servers and mark decommissioned servers inactive
         Object listener = this.nearListener;
         byte[] listenerId = this.nearListenerId;
         if (listener != null && listenerId != null) {
            Map<SocketAddress, Set<Integer>> primarySegments = newCacheInfo.getPrimarySegments();
            if (primarySegments != null) {
               for (Iterator<Map.Entry<SocketAddress, ServerListenerState>> it = serverListenerStates.entrySet().iterator(); it.hasNext(); ) {
                  Map.Entry<SocketAddress, ServerListenerState> entry = it.next();
                  if (!primarySegments.containsKey(entry.getKey())) {
                     ServerListenerState state = entry.getValue();
                     boolean wasActive = state.isActive();
                     state.setActive(false);
                     state.setChannel(null);
                     state.stopRegistering();
                     it.remove();
                     if (wasActive) {
                        SocketAddress server = entry.getKey();
                        try {
                           ClientListenerNotifier cln = getDispatcher().getClientListenerNotifier();
                           HotRodOperation<Void> op = getOperationsFactory().newRemoveClientListenerOperation(listenerId);
                           op.asCompletableFuture().whenComplete((___, t) -> cln.removeClientListener(listenerId, server));
                           getDispatcher().executeOnSingleAddress(op, server);
                        } catch (Exception e) {
                           if (trace) {
                              log.tracef(e, "Error removing primary listener on decommissioned server %s", server);
                           }
                        }
                     }
                  }
               }
               for (SocketAddress server : primarySegments.keySet()) {
                  registerPrimaryListenerOnServer(server, listener, listenerId, nearBloomBits)
                        .whenComplete((ch, t) -> {
                           if (t != null) {
                              getDispatcher().getClientListenerNotifier().scheduleReconnect(listenerId, server);
                           }
                        });
               }
            }
         }
      }
   }

   @Override
   public Marshaller getMarshaller() {
      return delegate.getMarshaller();
   }
}
