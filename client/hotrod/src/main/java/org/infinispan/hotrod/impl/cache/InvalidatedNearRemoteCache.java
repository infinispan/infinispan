package org.infinispan.hotrod.impl.cache;

import static org.infinispan.hotrod.impl.Util.await;
import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.lang.invoke.MethodHandles;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.hotrod.impl.operations.ClientListenerOperation;
import org.infinispan.hotrod.impl.operations.RetryAwareCompletionStage;
import org.infinispan.hotrod.impl.operations.UpdateBloomFilterOperation;
import org.infinispan.hotrod.near.NearCacheService;

/**
 * Near cache implementation enabling
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
   // representation. This value will be non null when bloom filter is enabled, otherwise null. When this
   // value is even it means there is no bloom filter update being sent and the near cache can be updated
   // locally from reads. When a bloom filter is being replicated though the value will be odd and the
   // near cache cannot be updated.
   private final AtomicInteger bloomFilterUpdateVersion;
   private volatile SocketAddress listenerAddress;

   InvalidatedNearRemoteCache(RemoteCache<K, V> remoteCache, ClientStatistics clientStatistics,
                              NearCacheService<K, V> nearcache) {
      super(remoteCache);
      this.clientStatistics = clientStatistics;
      this.nearcache = nearcache;
      this.bloomFilterUpdateVersion = nearcache.getBloomFilterBits() > 0 ? new AtomicInteger() : null;
   }

   @Override
   <Key, Value> RemoteCache<Key, Value> newDelegatingCache(RemoteCache<Key, Value> innerCache) {
      return new InvalidatedNearRemoteCache<>(innerCache, clientStatistics, (NearCacheService<Key, Value>) nearcache);
   }

   public static <K, V> InvalidatedNearRemoteCache<K, V> delegatingNearCache(RemoteCacheImpl<K, V> remoteCache,
                                                                             NearCacheService<K, V> nearCacheService) {
      return new InvalidatedNearRemoteCache<>(remoteCache, remoteCache.getClientStatistics(), nearCacheService);
   }

   @Override
   public CompletionStage<V> get(K key, CacheOptions options) {
      return getEntry(key, options).thenApply(v -> v != null ? v.value() : null);
   }

   private int getCurrentVersion() {
      if (bloomFilterUpdateVersion != null) {
         return bloomFilterUpdateVersion.get();
      }
      return 0;
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getEntry(K key, CacheOptions options) {
      CacheEntry<K, V> nearValue = nearcache.get(key);
      if (nearValue == null) {
         clientStatistics.incrementNearCacheMisses();
         int prevVersion = getCurrentVersion();
         RetryAwareCompletionStage<CacheEntry<K, V>> remoteValue = super.getEntry(key, options, listenerAddress);
         return remoteValue.thenApply(e -> {
            // We cannot cache the value if a retry was required - which means we did not talk to the listener node
            if (e != null) {
               // If previous version is odd we can't cache as that means it was started during
               // a bloom filter update. We also can't cache if the new version doesn't match the prior
               // as it overlapped a bloom update.
               if ((prevVersion & 1) == 1 || prevVersion != getCurrentVersion()) {
                  if (trace) {
                     log.tracef("Unable to cache returned value for key %s as operation was performed during a" +
                           " bloom filter update");
                  }
               }
               // Having a listener address means it has a bloom filter. When we have a bloom filter we cannot
               // cache values upon a retry as we can't guarantee the bloom filter is updated on the server properly
               else if (listenerAddress != null && remoteValue.wasRetried()) {
                  if (trace) {
                     log.tracef("Unable to cache returned value for key %s as operation was retried", key);
                  }
               } else {
                  nearcache.putIfAbsent(e);
                  e.metadata().expiration().maxIdle().ifPresent(m -> HOTROD.nearCacheMaxIdleUnsupported());
               }
            }
            return e;
         });
      } else {
         clientStatistics.incrementNearCacheHits();
         return CompletableFuture.completedFuture(nearValue);
      }
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> put(K key, V value, CacheWriteOptions options) {
      options.expiration().maxIdle().ifPresent(m -> HOTROD.nearCacheMaxIdleUnsupported());
      return super.put(key, value, options).thenApply(v -> {
         nearcache.remove(key);
         return v;
      });
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> putIfAbsent(K key, V value, CacheWriteOptions options) {
      options.expiration().maxIdle().ifPresent(m -> HOTROD.nearCacheMaxIdleUnsupported());
      return super.putIfAbsent(key, value, options).thenApply(v -> {
         nearcache.remove(key);
         return v;
      });
   }

   @Override
   public CompletionStage<Boolean> setIfAbsent(K key, V value, CacheWriteOptions options) {
      options.expiration().maxIdle().ifPresent(m -> HOTROD.nearCacheMaxIdleUnsupported());
      return super.setIfAbsent(key, value, options).thenApply(v -> {
         nearcache.remove(key);
         return v;
      });
   }

   @Override
   public CompletionStage<Void> set(K key, V value, CacheWriteOptions options) {
      options.expiration().maxIdle().ifPresent(m -> HOTROD.nearCacheMaxIdleUnsupported());
      return super.set(key, value, options).thenApply(v -> {
         nearcache.remove(key);
         return v;
      });
   }

   @Override
   public CompletionStage<Boolean> replace(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      options.expiration().maxIdle().ifPresent(m -> HOTROD.nearCacheMaxIdleUnsupported());
      return super.replace(key, value, version, options).thenApply(v -> {
         if (v) {
            nearcache.remove(key);
         }
         return v;
      });
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      options.expiration().maxIdle().ifPresent(m -> HOTROD.nearCacheMaxIdleUnsupported());
      return super.getOrReplaceEntry(key, value, version, options).thenApply(v -> {
         nearcache.remove(key);
         return v;
      });
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheOptions options) {
      return super.remove(key, options).thenApply(v -> {
         if (v) {
            nearcache.remove(key);
         }
         return v;
      });
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheEntryVersion version, CacheOptions options) {
      return super.remove(key, version, options).thenApply(v -> {
         if (v) {
            nearcache.remove(key);
         }
         return v;
      });
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getAndRemove(K key, CacheOptions options) {
      return super.getAndRemove(key, options).thenApply(v -> {
         nearcache.remove(key);
         return v;
      });
   }

   @Override
   public CompletionStage<Void> clear(CacheOptions options) {
      return super.clear(options).thenRun(nearcache::clear);
   }

   @Override
   public CompletionStage<Void> putAll(Map<K, V> entries, CacheWriteOptions options) {
      options.expiration().maxIdle().ifPresent(m -> HOTROD.nearCacheMaxIdleUnsupported());
      return super.putAll(entries, options).thenRun(() -> entries.keySet().forEach(nearcache::remove));
   }

   public void start() {
      listenerAddress = nearcache.start(this);
   }

   public void stop() {
      nearcache.stop(this);
   }

   public void clearNearCache() {
      nearcache.clear();
   }

   // Increments the bloom filter version if it is even and returns whether it was incremented
   private boolean incrementBloomVersionIfEven() {
      if (bloomFilterUpdateVersion != null) {
         int prev;
         do {
            prev = bloomFilterUpdateVersion.get();
            // Odd number means we are already sending bloom filter
            if ((prev & 1) == 1) {
               return false;
            }
         } while (!bloomFilterUpdateVersion.compareAndSet(prev, prev + 1));
      }
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
               org.infinispan.commons.util.Util.printArray(bloomFilterBits), listenerAddress,
               org.infinispan.commons.util.Util.printArray(nearcache.getListenerId()));
      }
      CacheOperationsFactory cacheOperationsFactory = getOperationsFactory();
      UpdateBloomFilterOperation bloopOp = cacheOperationsFactory.newUpdateBloomFilterOperation(CacheOptions.DEFAULT, listenerAddress, bloomFilterBits);
      return incrementBloomVersionUponCompletion(bloopOp.execute());
   }

   public SocketAddress getBloomListenerAddress() {
      return listenerAddress;
   }

   public void setBloomListenerAddress(SocketAddress socketAddress) {
      this.listenerAddress = socketAddress;
   }

   @Override
   public SocketAddress addNearCacheListener(Object listener, int bloomBits) {
      ClientListenerOperation op = getOperationsFactory().newAddNearCacheListenerOperation(listener, CacheOptions.DEFAULT, getDataFormat(),
            bloomBits, this);
      // no timeout, see below
      return await(op.execute());
   }
}
