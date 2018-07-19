package org.infinispan.client.hotrod.near;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;

/**
 * {@link java.util.LinkedHashMap} based near cache implementation.
 * Concurrent access is controlled by a reentrant RW lock.
 *
 * @since 7.1
 * @deprecated Use {@link BoundedConcurrentMapNearCache} instead
 */
@Deprecated
final class LinkedMapNearCache<K, V> implements NearCache<K, V> {

   private final LinkedHashMap<K, MetadataValue<V>> cache;

   private final ReadWriteLock rwlock = new ReentrantReadWriteLock();

   protected LinkedMapNearCache(LinkedHashMap<K, MetadataValue<V>> cache) {
      this.cache = cache;
   }

   @Override
   public void put(K key, MetadataValue<V> value) {
      Lock lock = rwlock.writeLock();
      try {
         lock.lock();
         cache.put(key, value);
      } finally {
         lock.unlock();
      }
   }

   @Override
   public void putIfAbsent(K key, MetadataValue<V> value) {
      Lock lock = rwlock.writeLock();
      try {
         lock.lock();
         MetadataValue<V> current = cache.get(key);
         if (current == null)
            cache.put(key, value);
      } finally {
         lock.unlock();
      }
   }

   @Override
   public void remove(K key) {
      Lock lock = rwlock.writeLock();
      try {
         lock.lock();
         cache.remove(key);
      } finally {
         lock.unlock();
      }
   }

   @Override
   public MetadataValue<V> get(K key) {
      Lock lock = rwlock.readLock();
      try {
         lock.lock();
         return cache.get(key);
      } finally {
         lock.unlock();
      }
   }

   @Override
   public void clear() {
      Lock lock = rwlock.writeLock();
      try {
         lock.lock();
         cache.clear();
      } finally {
         lock.unlock();
      }
   }

   public static <K, V> NearCache<K, V> create(final NearCacheConfiguration config) {
      return new LinkedMapNearCache(
            new LinkedHashMap<K, MetadataValue<V>>(1 << 4, 0.75f, true) {
         @Override
         protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > config.maxEntries();
         }
      });
   }

}
