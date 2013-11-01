package org.infinispan.jcache.util;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.concurrent.jdk8backported.LongAdder;

import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public class InMemoryJCacheLoader<K, V> implements CacheLoader<K, V> {

   private final ConcurrentMap<K, V> store = CollectionFactory.makeConcurrentMap();

   private final LongAdder counter = new LongAdder();

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
