package org.infinispan.client.hotrod.impl;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.lang.invoke.MethodHandles;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.client.hotrod.impl.operations.ClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.GetWithMetadataOperation;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.near.NearCacheService;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.concurrent.CompletableFutures;

import io.netty.channel.Channel;

/**
 * Near {@link org.infinispan.client.hotrod.RemoteCache} implementation enabling
 *
 * @param <K>
 * @param <V>
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
      if (nearValue == null || nearValue.getValue() == null) {
         clientStatistics.incrementNearCacheMisses();
         // Note that MetadataValueImpl does not implement equals method so we rely upon object identity in the replace below
         // We cannot cache this value as we could have 2 concurrent gets and an update and we could cache a previous one
         MetadataValue<V> calculatingPlaceholder = new MetadataValueImpl<>(-1, -1, -1, -1, -1, null);
         boolean cache = nearcache.putIfAbsent(key, calculatingPlaceholder);
         int prevVersion = getCurrentVersion();
         CompletionStage<GetWithMetadataOperation.GetWithMetadataResult<V>> remoteValue = super.getWithMetadataAsync(key, listenerChannel);
         // If previous version is odd we can't cache as that means it was started during
         // a bloom filter update.
         if (!cache || (prevVersion & 1) == 1) {
            if (trace) {
               log.tracef("Unable to cache returned value for key %s as either has concurrent operation or during a bloom filter update",
                     org.infinispan.commons.util.Util.toStr(key));
            }
            nearcache.remove(key, calculatingPlaceholder);
            return remoteValue.toCompletableFuture()
                  .thenApply(GetWithMetadataOperation.GetWithMetadataResult::value);
         }
         return remoteValue.thenApply(v -> {
            boolean shouldRemove = true;
            MetadataValue<V> value = v != null ? v.value() : null;
            // We cannot cache the value if a retry was required - which means we did not talk to the listener node
            if (value != null) {
               // We can't cache if the new version doesn't match the prior as it overlapped a bloom update.
               if (prevVersion != getCurrentVersion()) {
                  if (trace) {
                     log.tracef("Unable to cache returned value for key %s as operation was performed during a" +
                           " bloom filter update", org.infinispan.commons.util.Util.toStr(key));
                  }
               } else if (listenerChannel != null && v.retried()) {
                  // Having a listener address means it has a bloom filter. When we have a bloom filter we cannot
                  // cache values upon a retry as we can't guarantee the bloom filter is updated on the server properly
                  if (trace) {
                     log.tracef("Unable to cache returned value for key %s as operation was retried",
                           org.infinispan.commons.util.Util.toStr(key));
                  }
               } else {
                  nearcache.replace(key, calculatingPlaceholder, value);
                  if (value.getMaxIdle() > 0) {
                     HOTROD.nearCacheMaxIdleUnsupported();
                  }
                  shouldRemove = false;
               }
            }
            if (shouldRemove) {
               nearcache.remove(key, calculatingPlaceholder);
            }
            return value;
         }).toCompletableFuture();
      } else {
         clientStatistics.incrementNearCacheHits();
         return CompletableFuture.completedFuture(nearValue);
      }
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
      if (key != null && !delegate.getRemoteCacheManager().getMarshaller().mediaType().match(key)) {
         // The server has a storage type which the cache marshaller does not handle.
         // This could lead to losing events in the bloom filter, where the client and server see the key differently.
         // To avoid this issue, the client negotiate the type when installing the listener, but it needs a proper configuration.
         Class<? extends Marshaller> marshallerClass = delegate.getRemoteCacheManager().getMarshaller().getClass();
         log.invalidateNearDefaultMarshallerMismatch(delegate.getName(), marshallerClass, key);
         listenerChannel = nearcache.start(this);
         delegate.resolveStorage(key, value);
      } else {
         delegate.resolveStorage(key, value);
         listenerChannel = nearcache.start(this);
      }
   }

   @Override
   public void stop() {
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
      if (bloomFilterUpdateVersion == null) {
         return CompletableFutures.completedNull();
      }
      // Not being able to increment the even version means we have a concurrent update - so skip
      if (!incrementBloomVersionIfEven()) {
         if (trace) {
            log.tracef("Already have a concurrent bloom filter update for listenerId(%s) - skipping",
                  org.infinispan.commons.util.Util.printArray(nearcache.getListenerId()));
         }
         return CompletableFuture.completedFuture(null);
      }
      byte[] bloomFilterBits = nearcache.calculateBloomBits();

      if (trace) {
         log.tracef("Sending bloom filter bits(%s) update to %s for listenerId(%s)",
               org.infinispan.commons.util.Util.printArray(bloomFilterBits), listenerChannel,
               org.infinispan.commons.util.Util.printArray(nearcache.getListenerId()));
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
      ClientListenerOperation op = getOperationsFactory().newAddNearCacheListenerOperation(listener,
            bloomBits);
      return getDispatcher().await(getDispatcher().executeAddListener(op));
   }
}
