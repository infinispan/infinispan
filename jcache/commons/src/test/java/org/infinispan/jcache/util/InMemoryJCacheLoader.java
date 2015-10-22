package org.infinispan.jcache.util;

import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public class InMemoryJCacheLoader<K, V> implements CacheLoader<K, V> {

   private final ConcurrentMap<K, V> store;

   private final LongAdder counter = new LongAdder();

   public InMemoryJCacheLoader(ConcurrentMap<K, V> store) {
      this.store = store;
   }

   public static <K, V> InMemoryJCacheLoader<K, V> create() {
      return create(new ConcurrentHashMap<>());
   }

   public static <K, V> InMemoryJCacheLoader<K, V> create(ConcurrentMap<K, V> store) {
      return new InMemoryJCacheLoader<>(store);
   }

   public InMemoryJCacheLoader<K, V> store(K key, V value) {
      store.put(key, value);
      return this;
   }

   @Override
   public V load(K key) throws CacheLoaderException {
      V value = store.get(key);
      if (value != null)
         counter.increment();

      return value;
   }

   @Override
   public Map<K, V> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
      Map<K, V> values = new HashMap<K, V>();
      for (K key : keys)
         values.put(key, load(key));

      return values;
   }

   public long getLoadCount() {
      return counter.longValue();
   }

}
