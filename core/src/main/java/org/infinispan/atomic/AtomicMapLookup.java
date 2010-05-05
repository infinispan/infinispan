package org.infinispan.atomic;

import org.infinispan.Cache;

/**
 * A helper that locates atomic maps within a given cache.  This should be the <b>only</b> way AtomicMaps are created/retrieved.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class AtomicMapLookup {

   /**
    * Retrieves an atomic map from a given cache, stored under a given key.  If an AtomicMap did not exist, one is created
    * and registered in an atomic fashion.
    * @param cache underlying cache
    * @param <K> key param of the AtomicMap
    * @param <V> value param of the AtomicMap
    * @return an AtomicMap
    */
   @SuppressWarnings("unchecked")
   public static <MK, K, V> AtomicMap<K, V> getAtomicMap(Cache<?, ?> cache, MK key) {
      Object value = cache.get(key);
      if (value == null) value = AtomicHashMap.newInstance();
      AtomicHashMap<K, V> castValue = (AtomicHashMap<K, V>) value;
      return castValue.getProxy(cache, key, cache.getAdvancedCache().getBatchContainer(), cache.getAdvancedCache().getInvocationContextContainer());
   }
}
