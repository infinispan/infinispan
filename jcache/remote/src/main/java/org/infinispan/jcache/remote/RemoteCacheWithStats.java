package org.infinispan.jcache.remote;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;

public class RemoteCacheWithStats<K, V> extends RemoteCacheWrapper<K, V> {
   private LocalStatistics stats;

   public RemoteCacheWithStats(RemoteCache<K, V> delegate, LocalStatistics stats) {
      super(delegate);
      this.stats = stats;
   }

   @Override
   public V get(Object key) {
      V v = delegate.get(key);
      if (v == null) {
         stats.incrementCacheMisses();
      } else {
         stats.incrementCacheHits();
      }
      stats.incrementCacheGets();
      return v;
   }

   @Override
   public V put(K key, V value) {
      V v = delegate.put(key, value);
      stats.incrementCachePuts();
      return v;
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      V v = delegate.put(key, value, lifespan, unit);
      stats.incrementCachePuts();
      return v;
   }

   @Override
   public V putIfAbsent(K key, V value) {
      V result = delegate.putIfAbsent(key, value);
      if (result == null) {
         stats.incrementCachePuts();
      }
      return result;
   }

   @Override
   public V replace(K key, V value) {
      V v = delegate.replace(key, value);
      if (v != null) {
         stats.incrementCacheHits();
         stats.incrementCachePuts();
      } else {
         stats.incrementCacheMisses();
      }
      return v;
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version) {
      boolean replaced = delegate.replaceWithVersion(key, newValue, version);
      if (replaced) {
         stats.incrementCachePuts();
      }
      return replaced;
   }

   @Override
   public V remove(Object key) {
      V v = delegate.remove(key);
      if (v != null) {
         stats.incrementCacheRemovals();
      }
      return v;
   }

   @Override
   public boolean removeWithVersion(K key, long version) {
      boolean removed = delegate.removeWithVersion(key, version);
      if (removed) {
         stats.incrementCacheRemovals();
      }
      return removed;
   }
}
