package org.infinispan.client.hotrod.impl;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

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
import org.infinispan.client.hotrod.impl.transport.netty.NearCacheTopologyListener;
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
 * fails on every owner or the server does not support protocol 4.2, it falls back to a single cluster-wide listener.
 * <p>
 * Each server gets one registration attempt when first tracked as a primary owner. A failed registration or lost
 * listener disables near caching for that owner's segments, while healthy owners remain cacheable. Neither
 * subsequent topology updates nor transport reconnection retry the listener, including a server restart at the
 * same address while that address remains tracked as a primary owner. Reads for disabled owners go to the server.
 * <p>
 * Losing all primary segments removes the owner's listener state. If that server later becomes a primary owner
 * again, it gets a fresh registration attempt. Newly added primary owners also get an attempt.
 * <p>
 * During rolling upgrades the effective cluster protocol version may change at runtime.  The cache
 * automatically downgrades to the cluster-wide listener when an older-protocol server joins. The downgrade
 * is permanent: the cluster-wide listener works with every protocol version, so primary mode is never
 * re-entered.
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

   private static class ServerListenerState {
      private volatile Channel channel;
      private final AtomicInteger bloomVersion = new AtomicInteger();
      private final CompletableFuture<Channel> registration = new CompletableFuture<>();

      private boolean isActive() {
         Channel ch = this.channel;
         return ch != null && ch.isActive();
      }

      private Channel getChannel() {
         return channel;
      }

      private void setActive(Channel channel) {
         this.channel = channel;
      }

      private void setInactive() {
         this.channel = null;
      }

      private int getVersion() {
         return bloomVersion.get();
      }

      /**
       * Makes the version odd, so reads can't populate the near cache while the bloom filter is updated.
       *
       * @return the odd version that this update owns and has to release, or -1 if another update owns the version
       */
      private int claimVersion() {
         return claim(bloomVersion);
      }

      /** Makes the version even again, only if it still is the version claimed by the caller. */
      private void releaseVersion(int claimed) {
         release(bloomVersion, claimed);
      }

      private void bumpToNextEvenVersion() {
         int prev;
         int next;
         do {
            prev = bloomVersion.get();
            // Handle overflow: wrap to 0 instead of negative to maintain even parity
            if (prev >= Integer.MAX_VALUE - 2) {
               next = 0;
            } else {
               next = (prev & 1) == 1 ? prev + 1 : prev + 2;
            }
         } while (!bloomVersion.compareAndSet(prev, next));
      }
   }

   private static int claim(AtomicInteger version) {
      int prev;
      do {
         prev = version.get();
         // Odd number means another update is being sent
         if ((prev & 1) == 1) {
            return -1;
         }
      } while (!version.compareAndSet(prev, prev + 1));
      return prev + 1;
   }

   private static void release(AtomicInteger version, int claimed) {
      // A newer update or a reconnect may have advanced the version, which this one must leave alone
      version.compareAndSet(claimed, claimed + 1);
   }

   /** A bloom filter update that owns the odd version of a listener, until it releases it. */
   private record ClaimedVersion(ServerListenerState state, SocketAddress address, int version) { }

   // Guards reserving and activating listeners and detaching servers that lost their segments.
   // Network operations and waits never happen while held.
   private final Object primaryListenerLock = new Object();
   // Retain inactive states while their servers remain primary owners: presence records an attempted registration,
   // not just a live listener. Removing a failed owner's state would make the next topology retry its listener.
   private final ConcurrentMap<SocketAddress, ServerListenerState> serverListenerStates = new ConcurrentHashMap<>();
   // Guarded by primaryListenerLock. The last registration or removal queued for a server. They all use the same
   // listener ID, so they run one after the other, and an old cleanup can't remove a newer registration.
   private final Map<SocketAddress, CompletableFuture<Void>> serverTails = new HashMap<>();
   private final NearCacheTopologyListener topologyListener = new NearCacheTopologyListener() {
      @Override
      public void beforeTopologyUpdate(CacheInfo previous, CacheInfo next) {
         onPreTopologyUpdate(previous, next);
      }

      @Override
      public void primaryOwnersChanged(BitSet changedSegments) {
         onPrimaryOwnersChanged(changedSegments);
      }

      @Override
      public void afterTopologyUpdate(CacheInfo previous, CacheInfo next) {
         onTopologyUpdate(previous, next);
      }
   };
   private volatile Object nearListener;
   private volatile byte[] nearListenerId;
   private enum State {
      PRIMARY,
      CLUSTER
   }

   // Tracks the current operating mode of the near cache.
   private final AtomicReference<State> state = new AtomicReference<>(State.CLUSTER);
   private final AtomicInteger modeGeneration = new AtomicInteger(0);
   // Registered with OperationDispatcher to be notified when any channel's codec is negotiated.
   private Runnable codecChangeListener;
   // Guards concurrent calls to addNearCacheListener / switchToClusterListenerMode.
   private final AtomicBoolean modeSwitch = new AtomicBoolean(false);
   // Set while a downgrade is in progress, until the replacement cluster listener is installed. No entry is served
   // from or stored in the near cache then, as nothing invalidates it.
   private volatile boolean nearCacheSuspended;

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

   /**
    * Checks whether primary near cache mode is currently enabled and supported by the connected cluster.
    *
    * @return {@code true} if running in primary mode with known segment ownership, {@code false} otherwise
    */
   public boolean isPrimaryMode() {
      return state.get() == State.PRIMARY;
   }

   /**
    * Checks if at least one primary owner server has an active listener connected.
    */
   public boolean hasActivePrimaryListeners() {
      return !serverListenerStates.isEmpty() && serverListenerStates.values().stream().anyMatch(ServerListenerState::isActive);
   }

   /**
    * Permanently disables primary mode for this cache session, tears down any active primary owner listeners
    * and resets the primary-mode tracking state. Called when the initial setup fails, or when the cache is
    * downgraded to the cluster-wide listener.
    */
   private void cleanupPrimaryMode() {
      byte[] lid;
      Set<SocketAddress> registered = new HashSet<>();
      // Fence registration completions before collecting active listeners. Network operations
      // and waits must remain outside this lock.
      synchronized (primaryListenerLock) {
         state.set(State.CLUSTER);
         modeGeneration.incrementAndGet();
         lid = this.nearListenerId;
         this.nearListenerId = null;
         serverListenerStates.forEach((server, listenerState) -> {
            if (listenerState.isActive()) {
               registered.add(server);
            }
            listenerState.setInactive();
         });
         serverListenerStates.clear();
      }
      getDispatcher().removeTopologyListener(getName(), topologyListener);
      if (lid != null) {
         removePrimaryListeners(lid, registered);
      }
      nearcache.setSegmentFunction(null, 0);
   }

   private void removePrimaryListeners(byte[] lid, Set<SocketAddress> servers) {
      // Registrations still in flight see that the listener was removed, and unregister it themselves
      AggregateCompletionStage<Void> aggregate = CompletionStages.aggregateCompletionStage();
      for (SocketAddress server : servers) {
         aggregate.dependsOn(unregisterExclusively(server, lid));
      }
      try {
         getDispatcher().await(aggregate.freeze());
      } catch (Exception e) {
         if (trace) log.tracef(e, "Error waiting for primary listeners removal");
      }
      getDispatcher().getClientListenerNotifier().removeClientListener(lid);
   }

   /**
    * Runs the task once all the registrations and removals queued earlier for the server are complete.
    * Registrations and removals for a server never overlap.
    */
   private <T> CompletableFuture<T> runExclusively(SocketAddress server, Supplier<CompletionStage<T>> task) {
      CompletableFuture<T> result = new CompletableFuture<>();
      CompletableFuture<Void> done = new CompletableFuture<>();
      CompletableFuture<Void> previous;
      synchronized (primaryListenerLock) {
         previous = serverTails.put(server, done);
      }
      Runnable run = () -> {
         CompletionStage<T> stage;
         try {
            stage = task.get();
         } catch (Throwable t) {
            stage = CompletableFuture.failedFuture(t);
         }
         stage.whenComplete((value, t) -> {
            synchronized (primaryListenerLock) {
               serverTails.remove(server, done);
            }
            done.complete(null);
            if (t != null) {
               result.completeExceptionally(t);
            } else {
               result.complete(value);
            }
         });
      };
      if (previous == null) {
         run.run();
      } else {
         previous.whenComplete((ignore, t) -> run.run());
      }
      return result;
   }

   private CompletionStage<Void> unregisterExclusively(SocketAddress server, byte[] listenerId) {
      return runExclusively(server, () -> unregisterFromServer(server, listenerId));
   }

   /** Removes the listener from the server. This is best effort, the returned stage never fails. */
   private CompletionStage<Void> unregisterFromServer(SocketAddress server, byte[] listenerId) {
      if (server == null) {
         return CompletableFutures.completedNull();
      }
      ClientListenerNotifier cln = getDispatcher().getClientListenerNotifier();
      try {
         HotRodOperation<Void> op = getOperationsFactory().newRemoveClientListenerOperation(listenerId);
         return getDispatcher().executeOnSingleAddress(op, server)
               .handle((___, t) -> {
                  if (t != null && trace) {
                     log.tracef(t, "Error removing primary listener on %s", server);
                  }
                  cln.removeClientListener(listenerId, server);
                  getDispatcher().removeListener(server, listenerId);
                  return (Void) null;
               });
      } catch (Exception e) {
         if (trace) log.tracef(e, "Error sending remove primary listener to %s", server);
         cln.removeClientListener(listenerId, server);
         getDispatcher().removeListener(server, listenerId);
         return CompletableFutures.completedNull();
      }
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

   // Invoked under primaryListenerLock, only for newly tracked owners.
   private ServerListenerState createServerListenerState(SocketAddress server) {
      assertUnresolved(server);
      assert !serverListenerStates.containsKey(server);
      ServerListenerState listenerState = new ServerListenerState();
      serverListenerStates.put(server, listenerState);
      return listenerState;
   }

   private ServerListenerState getServerListenerState(SocketAddress server) {
      if (server == null) {
         return null;
      }
      assertUnresolved(server);
      return serverListenerStates.get(server);
   }

   /**
    * Handles the loss of the listener connection to a primary owner. The listener is marked unusable and the
    * segments the owner is primary for are fenced, so nothing stale is served or stored for them. Segments of the
    * other owners stay cacheable. The listener is not retried while the server remains a primary owner.
    */
   public void handlePrimaryListenerDisconnected(SocketAddress address) {
      handlePrimaryListenerDisconnected(address, null);
   }

   /**
    * @param channel the closed listener channel, or {@code null} if it is not known. A channel that is no longer
    *                the one of the listener, because it was replaced, is ignored.
    */
   public void handlePrimaryListenerDisconnected(SocketAddress address, Channel channel) {
      if (address == null) {
         return;
      }
      assertUnresolved(address);
      synchronized (primaryListenerLock) {
         ServerListenerState listenerState = serverListenerStates.get(address);
         if (listenerState != null) {
            Channel current = listenerState.getChannel();
            if (channel != null && current != null && current != channel) {
               return;
            }
            listenerState.setInactive();
            listenerState.bumpToNextEvenVersion();
         }
      }
      invalidateOwnerSegments(address);
   }

   private void invalidateOwnerSegments(SocketAddress address) {
      Map<SocketAddress, Set<Integer>> primarySegments = getDispatcher().getPrimarySegmentsByAddress(getName());
      if (primarySegments != null) {
         Set<Integer> segments = primarySegments.get(address);
         if (segments != null) {
            nearcache.invalidateSegments(segments);
         }
      }
   }

   /**
    * Stops using the listener of an owner after a failure that leaves its server side state unknown. The listener is
    * removed from the server. Its segments remain uncached while the server remains a primary owner.
    */
   private void disablePrimaryOwner(SocketAddress address, ServerListenerState listenerState) {
      byte[] listenerId;
      synchronized (primaryListenerLock) {
         if (serverListenerStates.get(address) != listenerState) {
            return;
         }
         // The update that failed releases its own version
         listenerState.setInactive();
         listenerId = nearListenerId;
      }
      invalidateOwnerSegments(address);
      if (listenerId != null) {
         unregisterExclusively(address, listenerId);
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
      if (nearCacheSuspended) {
         // Nothing invalidates the near cache until the replacement listener is installed
         return super.getWithMetadataAsync(key, null).toCompletableFuture()
               .thenApply(GetWithMetadataOperation.GetWithMetadataResult::value);
      }
      MetadataValue<V> nearValue = nearcache.get(key);
      // We rely upon the fact that a MetadataValue will never be null from a remote lookup but our placeholder will be null
      if (nearValue != null && nearValue.getValue() != null) {
         clientStatistics.incrementNearCacheHits();
         return CompletableFuture.completedFuture(nearValue);
      }

      clientStatistics.incrementNearCacheMisses();
      // Each placeholder has a unique identity for the conditional replacement below.
      // We cannot cache this value as we could have 2 concurrent gets and an update and we could cache a previous one
      MetadataValue<V> calculatingPlaceholder = nearcache.createPlaceholder(key);
      boolean cache = nearcache.putIfAbsent(key, calculatingPlaceholder);

      int modeGen = modeGeneration.get();
      if (isPrimaryMode()) {
         SocketAddress primaryServer = getPrimaryOwner(key);
         ServerListenerState state = getServerListenerState(primaryServer);
         Channel primaryChannel = (state != null && state.isActive()) ? state.getChannel() : null;
         int prevVersion = state != null ? state.getVersion() : -1;
         boolean updating = (prevVersion & 1) == 1;

         // Double-check the primary owner didn't change to avoid TOCTOU races
         boolean primaryOwnerChanged = primaryServer == null || !Objects.equals(primaryServer, getPrimaryOwner(key));

         if (!cache || updating || primaryChannel == null || primaryOwnerChanged) {
            if (trace) {
               log.tracef("Unable to cache returned value for key %s (cache=%s, prevVersion=%d, primary=%s, changed=%s)",
                     Util.toStr(key), cache, prevVersion, primaryServer, primaryOwnerChanged);
            }
            nearcache.remove(key, calculatingPlaceholder);
            return super.getWithMetadataAsync(key, null).toCompletableFuture()
                  .thenApply(GetWithMetadataOperation.GetWithMetadataResult::value);
         }
         // Store channel to validate it hasn't changed after get completes
         Channel channelUsed = primaryChannel;
         return super.getWithMetadataAsync(key, primaryChannel)
               .thenApply(v -> storeInNearCacheOrRemove(key, calculatingPlaceholder, v,
                     isPrimaryResultValid(key, v, primaryServer, prevVersion, channelUsed, modeGen)))
               .toCompletableFuture();
      } else {
         Channel clusterChannel = this.listenerChannel;
         int prevVersion = getCurrentVersion();
         boolean hasBloom = nearcache.getBloomFilterBits() > 0;
         CompletionStage<GetWithMetadataOperation.GetWithMetadataResult<V>> remoteValue =
               super.getWithMetadataAsync(key, clusterChannel);
         if (!cache || (prevVersion & 1) == 1 || (hasBloom && clusterChannel == null)) {
            if (trace) {
               log.tracef("Unable to cache returned value for key %s as either has concurrent operation, missing bloom channel, or during a bloom filter update",
                     Util.toStr(key));
            }
            nearcache.remove(key, calculatingPlaceholder);
            return remoteValue.toCompletableFuture()
                  .thenApply(GetWithMetadataOperation.GetWithMetadataResult::value);
         }
         return remoteValue
               .thenApply(v -> storeInNearCacheOrRemove(key, calculatingPlaceholder, v,
                     isClusterResultValid(key, v, prevVersion, modeGen, clusterChannel)))
               .toCompletableFuture();
      }
   }

   private boolean isPrimaryResultValid(K key, GetWithMetadataOperation.GetWithMetadataResult<V> v,
                                        SocketAddress primaryServer, int prevVersion, Channel channelUsed, int modeGen) {
      if (v == null || v.value() == null) {
         return false;
      }
      if (modeGen != modeGeneration.get() || !isPrimaryMode() || nearCacheSuspended) {
         if (trace) {
            log.tracef("Unable to cache returned value for key %s as mode switched during read", Util.toStr(key));
         }
         return false;
      }
      if (!Objects.equals(primaryServer, getPrimaryOwner(key))) {
         if (trace) {
            log.tracef("Unable to cache returned value for key %s as primary owner changed from %s",
                  Util.toStr(key), primaryServer);
         }
         return false;
      }
      if (nearcache.getBloomFilterBits() > 0 && v.retried()) {
         if (trace) {
            log.tracef("Unable to cache returned value for key %s as operation was retried", Util.toStr(key));
         }
         return false;
      }
      ServerListenerState state = getServerListenerState(primaryServer);
      boolean valid = state != null && state.isActive() && state.getChannel() == channelUsed && state.getVersion() == prevVersion;
      if (!valid && trace) {
         log.tracef("Unable to cache returned value for key %s as listener state/version on %s changed",
               Util.toStr(key), primaryServer);
      }
      return valid;
   }

   private boolean isClusterResultValid(K key, GetWithMetadataOperation.GetWithMetadataResult<V> v, int prevVersion,
                                        int modeGen, Channel clusterChannel) {
      if (v == null || v.value() == null) {
         return false;
      }
      boolean hasBloom = nearcache.getBloomFilterBits() > 0;
      if (modeGen != modeGeneration.get() || isPrimaryMode() || nearCacheSuspended || (hasBloom && (clusterChannel == null || this.listenerChannel != clusterChannel))) {
         if (trace) {
            log.tracef("Unable to cache returned value for key %s as mode or cluster listener switched during read",
                  Util.toStr(key));
         }
         return false;
      }
      if (prevVersion != getCurrentVersion()) {
         if (trace) {
            log.tracef("Unable to cache returned value for key %s as operation was performed during a bloom filter update",
                  Util.toStr(key));
         }
         return false;
      }
      if (hasBloom && v.retried()) {
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
      boolean removingNearListener = Objects.equals(listener, this.nearListener);
      if (removingNearListener) {
         // Forget the listener regardless of the current mode so a later codec change cannot downgrade it
         this.nearListener = null;
         if (codecChangeListener != null) {
            getDispatcher().removeCodecChangeListener(codecChangeListener);
            codecChangeListener = null;
         }
         if (isPrimaryMode()) {
            cleanupPrimaryMode();
            return;
         }
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

   @Override
   public CompletionStage<Void> updateBloomFilter() {
      if (bloomFilterUpdateVersion == null && nearcache.getBloomFilterBits() <= 0) {
         return CompletableFutures.completedNull();
      }
      Function<byte[], CompletionStage<Void>> send;
      Runnable release;
      if (isPrimaryMode()) {
         // Make the versions odd first so reads cannot populate the near cache while the bits are calculated
         List<ClaimedVersion> claims = new ArrayList<>();
         for (Map.Entry<SocketAddress, ServerListenerState> entry : serverListenerStates.entrySet()) {
            ServerListenerState state = entry.getValue();
            if (!state.isActive()) {
               continue;
            }
            int claimed = state.claimVersion();
            if (claimed >= 0) {
               claims.add(new ClaimedVersion(state, entry.getKey(), claimed));
            } else if (trace) {
               log.tracef("Already have a concurrent bloom filter update for server %s - skipping", entry.getKey());
            }
         }
         if (claims.isEmpty()) {
            return CompletableFutures.completedNull();
         }
         send = bits -> bits == null ? CompletableFutures.completedNull() : sendPrimaryBloomFilters(bits, claims);
         release = () -> releaseClaims(claims);
      } else {
         // Not being able to claim the even version means we have a concurrent update - so skip
         int claimed = claim(bloomFilterUpdateVersion);
         if (claimed < 0) {
            if (trace) {
               log.tracef("Already have a concurrent bloom filter update for listenerId(%s) - skipping",
                     Util.printArray(nearcache.getListenerId()));
            }
            return CompletableFuture.completedFuture(null);
         }
         release = () -> release(bloomFilterUpdateVersion, claimed);
         send = this::sendClusterBloomFilter;
      }
      // All claimed versions are odd before the calculation, so reads cannot populate the near cache.
      CompletableFuture<byte[]> bitsFuture;
      try {
         bitsFuture = CompletableFuture.supplyAsync(nearcache::calculateBloomBits, getDispatcher().getExecutorService());
      } catch (Throwable t) {
         release.run();
         return CompletableFuture.failedFuture(t);
      }
      return bitsFuture.thenCompose(send).whenComplete((ignore, t) -> release.run());
   }

   private CompletionStage<Void> sendClusterBloomFilter(byte[] bloomFilterBits) {
      if (trace) {
         log.tracef("Sending bloom filter bits(%s) update to %s for listenerId(%s)",
               Util.printArray(bloomFilterBits), listenerChannel,
               Util.printArray(nearcache.getListenerId()));
      }
      HotRodOperation<Void> op = getOperationsFactory().newUpdateBloomFilterOperation(bloomFilterBits);
      return getDispatcher().executeOnSingleAddress(op, ChannelRecord.of(listenerChannel));
   }

   private static void releaseClaims(List<ClaimedVersion> claims) {
      claims.forEach(claim -> claim.state().releaseVersion(claim.version()));
   }

   /**
    * Sends the bloom filter to the owners. An owner the update can't be sent to is not usable any more: its segments
    * are fenced and its listener stays unused while the server remains a primary owner.
    */
   private CompletionStage<Void> sendPrimaryBloomFilters(byte[] bloomFilterBits, List<ClaimedVersion> claims) {
      AggregateCompletionStage<Void> completionStage = CompletionStages.aggregateCompletionStage();
      CacheOperationsFactory operationsFactory = getOperationsFactory();
      for (ClaimedVersion claim : claims) {
         CompletionStage<Void> stage;
         try {
            HotRodOperation<Void> op = operationsFactory.newUpdateBloomFilterOperation(bloomFilterBits);
            stage = getDispatcher().executeOnSingleAddress(op, claim.address());
         } catch (Throwable t) {
            stage = CompletableFuture.failedFuture(t);
         }
         completionStage.dependsOn(stage.whenComplete((ignore, t) -> {
            if (t != null) {
               log.tracef(t, "Failed to update bloom filter on primary server %s; disabling its listener", claim.address());
               disablePrimaryOwner(claim.address(), claim.state());
            }
            claim.state().releaseVersion(claim.version());
         }).toCompletableFuture());
      }
      return completionStage.freeze();
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

   public Channel addNearCacheListener(Object listener, int bloomBits) {
      this.nearListener = listener;

      // Register codec change listener once — reacts to rolling-upgrade downgrades to an older protocol version.
      if (codecChangeListener == null) {
         codecChangeListener = this::onCodecChange;
         getDispatcher().addCodecChangeListener(codecChangeListener);
      }

      // Guard with modeSwitch so that codec-change callbacks received while primary listener
      // registration is in flight (during the blocking await) do not race with this setup.
      // Use compareAndSet so that a concurrent call (e.g. from a reconnect) does not proceed
      // into setup simultaneously; the second caller simply falls back to the cluster listener.
      if (!modeSwitch.compareAndSet(false, true)) {
         return installClusterListener(listener, bloomBits);
      }
      try {
         if (canUsePrimaryMode()) {
            this.nearListenerId = ClientListenerOperation.generateListenerId();
            PrimaryNearCacheEventDispatcher nearDispatcher = PrimaryNearCacheEventDispatcher.create(
                  getOperationsFactory().newAddPrimaryNearCacheListenerOperation(listener, nearListenerId, bloomBits), this);
            getDispatcher().getClientListenerNotifier().addDispatcher(nearDispatcher);
            getDispatcher().addTopologyListener(getName(), topologyListener);
            CacheInfo startInfo = getDispatcher().getCacheInfo(getName());
            Channel primaryChannel = null;
            try {
               primaryChannel = registerPrimaryListeners(startInfo);
               if (primaryChannel == null) {
                  log.debugf("No primary listeners could be established for cache '%s'; falling back to cluster-wide listener", getName());
               }
            } catch (Throwable t) {
               log.debugf(t, "Failed to register primary listeners for cache '%s'; falling back to cluster-wide listener", getName());
            }
            // An older-protocol server may have connected while registering. The codec change callback ignored it
            // as we were not in primary mode yet, so verify before enabling primary mode.
            if (primaryChannel != null && canUsePrimaryMode()) {
               activatePrimaryMode(startInfo);
               return primaryChannel;
            }
            // If all primary listener registrations failed (or protocol is unsupported), fallback to cluster-wide listener mode.
            // A partial registration failure remains in primary mode for the reachable servers. The others are
            // left disabled while they remain primary owners.
            cleanupPrimaryMode();
         }
         return installClusterListener(listener, bloomBits);
      } finally {
         modeSwitch.set(false);
         // Reconcile with any codec change that happened while the initial setup was in progress
         onCodecChange();
      }
   }

   /**
    * Enables primary mode against the latest accepted topology. Topologies that were accepted while the listeners
    * were registering were not acted upon, as the cache was not in primary mode, so this reconciles unconditionally.
    * It is serialized with the topology updates, so that the snapshot taken at the start of the setup can't
    * overwrite the state of a newer topology.
    */
   private void activatePrimaryMode(CacheInfo startInfo) {
      getDispatcher().withLatestTopology(getName(), currentInfo -> {
         if (currentInfo != null && startInfo != null) {
            invalidateChangedSegments(startInfo.getConsistentHash(), currentInfo.getConsistentHash());
         }
         state.set(State.PRIMARY);
         modeGeneration.incrementAndGet();
         if (currentInfo != null) {
            reconcilePrimaryListeners(currentInfo);
         }
         return null;
      });
   }

   /**
    * Returns {@code true} if primary-owner listener mode is eligible at this point in time: the dispatcher
    * intelligence is HASH_DISTRIBUTION_AWARE, all connected channels have negotiated Hot Rod 4.2+, primary
    * segment information is available, and primary mode has not been permanently disabled.
    */
   private boolean canUsePrimaryMode() {
      if (!getDispatcher().isPrimaryNearCacheListenerSupported()) {
         return false;
      }
      Map<SocketAddress, Set<Integer>> primarySegments = getDispatcher().getPrimarySegmentsByAddress(getName());
      return primarySegments != null && !primarySegments.isEmpty();
   }

   /**
    * Registers a cluster-wide (non-primary) near cache listener and updates the tracked listener ID.
    *
    * @return the channel holding the listener if bloom filters are enabled (updates and reads must use it), otherwise {@code null}
    */
   private Channel installClusterListener(Object listener, int bloomBits) {
      ClientListenerOperation op = getOperationsFactory().newAddNearCacheListenerOperation(listener, bloomBits);
      Channel channel = getDispatcher().await(getDispatcher().executeAddListener(op));
      this.nearListenerId = op.listenerId;
      nearcache.updateListenerId(op.listenerId);
      return bloomBits > 0 ? channel : null;
   }

   /**
    * Registers primary near cache listeners across all servers owning primary segments in parallel. A server that
    * fails is not retried while it remains a primary owner.
    *
    * @return a currently active primary listener channel, or {@code null}
    */
   private Channel registerPrimaryListeners(CacheInfo startInfo) {
      OperationDispatcher dispatcher = getDispatcher();
      // Use the hash activation compares with the latest topology. A fresh lookup here could capture
      // an intervening segment count and leave the mapping wrong if the original count returns before activation.
      ConsistentHash ch = startInfo.getConsistentHash();
      if (ch instanceof SegmentConsistentHash sch) {
         nearcache.setSegmentFunction(k -> sch.getSegment(keyToBytes(k)), sch.getNumSegments());
      }
      Map<SocketAddress, Set<Integer>> primarySegments = dispatcher.getPrimarySegmentsByAddress(getName());
      if (primarySegments == null || primarySegments.isEmpty()) {
         return null;
      }

      AggregateCompletionStage<Void> aggregate = CompletionStages.aggregateCompletionStage();

      for (SocketAddress server : primarySegments.keySet()) {
         ServerListenerState listenerState;
         synchronized (primaryListenerLock) {
            listenerState = createServerListenerState(server);
         }
         CompletionStage<Channel> stage = registerPrimaryListenerOnServer(server, listenerState)
               .exceptionally(t -> null);
         aggregate.dependsOn(stage);
      }
      dispatcher.await(aggregate.freeze());
      return serverListenerStates.values().stream()
            .map(ServerListenerState::getChannel)
            .filter(channel -> channel != null && channel.isActive())
            .findAny().orElse(null);
   }

   /**
    * Registers a primary listener on a specific primary owner. This is only invoked by the initial setup and by
    * topology updates for newly added primary owners. A failed listener is not retried while its server remains
    * a primary owner.
    * <p>
    * The listener is activated only if, when the registration completes, it is still the listener of the cache, the
    * server is still attached to it and is still a primary owner, and its channel is alive. Otherwise the
    * registration is removed from the server. If bloom filters are enabled, the current local state is sent
    * before the listener is activated.
    */
   private CompletionStage<Channel> registerPrimaryListenerOnServer(SocketAddress server, ServerListenerState listenerState) {
      byte[] listenerId = nearListenerId;
      AtomicBoolean added = new AtomicBoolean();
      runExclusively(server, () -> register(server, listenerId, listenerState, added))
            .whenComplete((channel, t) -> {
               if (t != null) {
                  listenerState.registration.completeExceptionally(t);
               } else {
                  listenerState.registration.complete(channel);
               }
            });
      return listenerState.registration;
   }

   private CompletionStage<Channel> register(SocketAddress server, byte[] listenerId, ServerListenerState listenerState,
                                             AtomicBoolean added) {
      Throwable invalid = validateRegistration(server, listenerId, listenerState, null);
      if (invalid != null) {
         return CompletableFuture.failedFuture(invalid);
      }
      int bloomBits = nearcache.getBloomFilterBits();
      ClientListenerOperation op = getOperationsFactory().newAddPrimaryNearCacheListenerOperation(nearListener, listenerId, bloomBits);
      CompletionStage<Channel> registered;
      try {
         registered = getDispatcher().executeAddListener(op, server)
               .thenCompose(ch -> {
                  added.set(true);
                  return getDispatcher().primaryTopologyReady(getName(), ch);
               })
               .thenCompose(ch -> {
                  if (bloomBits > 0) {
                     // This runs on the event loop, so calculate the bloom bits (iterates the whole near cache) elsewhere
                     return CompletableFuture.supplyAsync(nearcache::calculateBloomBits, getDispatcher().getExecutorService())
                           .thenCompose(currentBloomBits -> {
                              if (currentBloomBits == null) {
                                 return CompletableFuture.completedFuture(ch);
                              }
                              HotRodOperation<Void> updateOp = getOperationsFactory().newUpdateBloomFilterOperation(currentBloomBits);
                              return getDispatcher().executeOnSingleAddress(updateOp, server)
                                    .thenApply(ignore -> ch);
                           });
                  }
                  return CompletableFuture.completedFuture(ch);
               });
      } catch (Throwable t) {
         registered = CompletableFuture.failedFuture(t);
      }
      return registered.handle((ch, t) -> {
         Throwable failure = t;
         synchronized (primaryListenerLock) {
            if (failure == null) {
               failure = validateRegistration(server, listenerId, listenerState, ch);
            }
            if (failure == null) {
               // The decision to activate is atomic with the detachment of the owner, which uses the same lock
               markListenerActive(listenerState, ch);
               return CompletableFuture.completedFuture(ch);
            }
            listenerState.setInactive();
         }
         if (log.isDebugEnabled()) {
            log.debugf(failure, "Failed to register primary listener on server %s", server);
         }
         // Complete the registration only after its server listener has been removed
         Throwable cause = failure;
         CompletionStage<Void> cleanup = added.get() ? unregisterFromServer(server, listenerId) : CompletableFutures.completedNull();
         return cleanup.<Channel>thenCompose(ignore -> CompletableFuture.failedFuture(cause));
      }).thenCompose(stage -> stage);
   }

   /**
    * Returns why the registration can't be activated, or {@code null} when it can. Must be invoked with
    * {@link #primaryListenerLock} held when deciding to activate.
    */
   private Throwable validateRegistration(SocketAddress server, byte[] listenerId, ServerListenerState listenerState,
                                          Channel channel) {
      boolean current;
      synchronized (primaryListenerLock) {
         current = nearListenerId == listenerId && serverListenerStates.get(server) == listenerState;
      }
      if (!current) {
         return new IllegalStateException("Primary listener removed or server detached");
      }
      Map<SocketAddress, Set<Integer>> primarySegments = getDispatcher().getPrimarySegmentsByAddress(getName());
      if (primarySegments == null || !primarySegments.containsKey(server)) {
         return new IllegalStateException("Server " + server + " is no longer a primary owner");
      }
      if (channel != null && !channel.isActive()) {
         return new IllegalStateException("Primary listener channel to " + server + " is closed");
      }
      return null;
   }

   private static void markListenerActive(ServerListenerState state, Channel ch) {
      state.setActive(ch);
      state.bumpToNextEvenVersion();
   }

   private void submitModeSwitch(Runnable modeSwitchTask) {
      try {
         getDispatcher().getExecutorService().submit(modeSwitchTask);
      } catch (RejectedExecutionException e) {
         ForkJoinPool.commonPool().submit(modeSwitchTask);
      }
   }

   /**
    * Reacts to a codec negotiation event fired by {@code HeaderDecoder} when any channel has its protocol
    * version finalised. If we are currently in primary mode but a channel now reports a codec that does not
    * support primary near cache listeners (e.g. a 4.1 server joined during a rolling upgrade), we downgrade
    * to the cluster-wide listener. The downgrade is permanent.
    */
   private void onCodecChange() {
      if (isPrimaryMode() && !getDispatcher().isPrimaryNearCacheListenerSupported()) {
         // Submit to the dispatcher executor to avoid blocking the Netty event loop.
         submitModeSwitch(this::switchToClusterListenerMode);
      }
   }

   /**
    * Permanently downgrades the near cache from primary-owner listener mode to a single cluster-wide listener.
    * Triggered when an older-protocol server joins the cluster during a rolling upgrade.
    * <p>
    * Nothing invalidates the near cache between removing the primary listeners and installing the cluster-wide
    * listener, so it is cleared and neither serves nor stores entries until the replacement listener is installed.
    * If installing it fails the near cache stays disabled.
    */
   private void switchToClusterListenerMode() {
      // If the guard is held, addNearCacheListener is running and re-evaluates the codec state when it completes
      if (!modeSwitch.compareAndSet(false, true)) {
         return;
      }
      try {
         if (!isPrimaryMode()) {
            return; // already in cluster mode, nothing to do
         }
         log.nearCacheDowngradedToClusterListener(getName());
         Object listener = this.nearListener;
         int bloomBits = nearcache.getBloomFilterBits();
         nearCacheSuspended = true;
         cleanupPrimaryMode();
         nearcache.clear();
         if (listener != null) {
            this.listenerChannel = installClusterListener(listener, bloomBits);
            // Removal may race with installation, before the replacement is visible to removeClientListener.
            if (this.nearListener != listener) {
               super.removeClientListener(listener);
            }
            modeGeneration.incrementAndGet();
            // Drop anything that was stored by a read that began before the near cache was suspended
            nearcache.clear();
         }
         nearCacheSuspended = false;
      } finally {
         modeSwitch.set(false);
      }
   }

   /**
    * Fences the segments whose primary owner changes, before the new owners are published. It runs under the
    * topology lock of the dispatcher, so no read can pair the new owners with the previous epochs.
    */
   void onPreTopologyUpdate(CacheInfo oldCacheInfo, CacheInfo newCacheInfo) {
      if (newCacheInfo == null || !isPrimaryMode()) {
         return;
      }
      ConsistentHash oldCH = oldCacheInfo != null ? oldCacheInfo.getConsistentHash() : null;
      invalidateChangedSegments(oldCH, newCacheInfo.getConsistentHash());
   }

   /**
    * Fences the segments whose primary ownership changed in the topologies that the server coalesced, which includes
    * ownership that changed and was restored, or all segments if the history is not known.
    */
   void onPrimaryOwnersChanged(BitSet changedSegments) {
      if (!isPrimaryMode()) {
         return;
      }
      if (changedSegments == null) {
         nearcache.invalidateAllSegments();
      } else {
         nearcache.invalidateSegments(changedSegments);
      }
   }

   private void invalidateChangedSegments(ConsistentHash oldCH, ConsistentHash newCH) {
      if (newCH instanceof SegmentConsistentHash newSCH) {
         if (!(oldCH instanceof SegmentConsistentHash oldSCH) || oldSCH.getNumSegments() != newSCH.getNumSegments()) {
            // A different segment count changes the key-to-segment mapping, even for unchanged owners.
            // Replacing the epoch array also fences values and in-flight placeholders from the old mapping.
            nearcache.setSegmentFunction(k -> newSCH.getSegment(keyToBytes(k)), newSCH.getNumSegments());
            return;
         }
         // Routing topologies derive their segment count from the owners array. CacheInfo has already
         // collected their primary owners, so every row here contains a primary address.
         SocketAddress[][] oldOwners = oldSCH.getSegmentOwners();
         SocketAddress[][] newOwners = newSCH.getSegmentOwners();
         for (int seg = 0; seg < newOwners.length; ++seg) {
            if (!Objects.equals(oldOwners[seg][0], newOwners[seg][0])) {
               nearcache.invalidateSegment(seg);
            }
         }
      }
   }

   /**
    * Removes listeners of servers that no longer own primary segments and registers newly added primary owners.
    * Existing owners without an active listener remain disabled, without retrying their registration.
    */
   void onTopologyUpdate(CacheInfo oldCacheInfo, CacheInfo newCacheInfo) {
      if (newCacheInfo == null || !isPrimaryMode()) {
         return;
      }
      if (newCacheInfo.getConsistentHash() instanceof SegmentConsistentHash) {
         reconcilePrimaryListeners(newCacheInfo);
      }
   }

   private void reconcilePrimaryListeners(CacheInfo cacheInfo) {
      Map<SocketAddress, Set<Integer>> primarySegments = cacheInfo.getPrimarySegments();
      if (primarySegments == null) {
         return;
      }
      byte[] listenerId;
      Map<SocketAddress, Boolean> detached = new HashMap<>();
      Map<SocketAddress, ServerListenerState> toRegister = new HashMap<>();
      // Detaching an owner and deciding about it are done under the lock that registrations are completed with
      synchronized (primaryListenerLock) {
         listenerId = this.nearListenerId;
         if (nearListener == null || listenerId == null || !isPrimaryMode()) {
            return;
         }
         for (Iterator<Map.Entry<SocketAddress, ServerListenerState>> it = serverListenerStates.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<SocketAddress, ServerListenerState> entry = it.next();
            if (!primarySegments.containsKey(entry.getKey())) {
               ServerListenerState listenerState = entry.getValue();
               detached.put(entry.getKey(), listenerState.isActive());
               listenerState.setInactive();
               it.remove();
            }
         }
         for (SocketAddress server : primarySegments.keySet()) {
            if (!serverListenerStates.containsKey(server)) {
               // Reserve the owner before releasing the lock, so another topology cannot schedule it again.
               toRegister.put(server, createServerListenerState(server));
            }
         }
      }
      // A registration in flight for a detached owner notices that it was detached, and removes itself
      detached.forEach((server, wasActive) -> {
         if (wasActive) {
            unregisterExclusively(server, listenerId);
         }
      });
      toRegister.forEach(this::registerPrimaryListenerOnServer);
   }

   @Override
   public Marshaller getMarshaller() {
      return delegate.getMarshaller();
   }
}
