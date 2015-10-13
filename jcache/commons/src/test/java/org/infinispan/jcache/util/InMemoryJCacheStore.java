package org.infinispan.jcache.util;

import javax.cache.Cache;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryJCacheStore<K, V> implements CacheLoader<K, V>, CacheWriter<K, V> {

   final ConcurrentMap<K, V> store;

   final InMemoryJCacheLoader<K, V> loader;

   public InMemoryJCacheStore() {
      this.store = new ConcurrentHashMap<>();
      this.loader = InMemoryJCacheLoader.create(this.store);
   }

   @Override
   public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
      store.put(entry.getKey(), entry.getValue());
   }

   @Override
   public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) throws CacheWriterException {
      entries.forEach(this::write);
   }

   @Override
   public void delete(Object key) throws CacheWriterException {
      store.remove(key);
   }

   @Override
   public void deleteAll(Collection<?> keys) throws CacheWriterException {
      keys.forEach(this::delete);
   }

   @Override
   public V load(K key) throws CacheLoaderException {
      return loader.load(key);
   }

   @Override
   public Map<K, V> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
      return loader.loadAll(keys);
   }
}
