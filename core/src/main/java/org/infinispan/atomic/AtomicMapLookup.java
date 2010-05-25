package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.util.Immutables;

import java.util.Collections;
import java.util.Map;

import static org.infinispan.util.Immutables.immutableMapWrap;

/**
 * A helper that locates atomic maps within a given cache.  This should be the <b>only</b> way AtomicMaps are created/retrieved.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class AtomicMapLookup {

   /**
    * Retrieves an atomic map from a given cache, stored under a given key.  If an atomic map did not exist, one is created
    * and registered in an atomic fashion.
    *
    * @param cache underlying cache
    * @param key key under which the atomic map exists
    * @param <MK> key param of the cache
    * @param <K> key param of the AtomicMap
    * @param <V> value param of the AtomicMap
    * @return an AtomicMap
    */
   @SuppressWarnings("unchecked")
   public static <MK, K, V> AtomicMap<K, V> getAtomicMap(Cache<MK, ?> cache, MK key) {
      return getAtomicMap(cache, key, true);
   }

   /**
    * Retrieves an atomic map from a given cache, stored under a given key.
    *
    * @param cache underlying cache
    * @param key key under which the atomic map exists
    * @param createIfAbsent if true, a new atomic map is created if one did not exist.
    * @param <MK> key param of the cache
    * @param <K> key param of the AtomicMap
    * @param <V> value param of the AtomicMap
    * @return an AtomicMap, or null if one did not exist.
    */
   @SuppressWarnings("unchecked")
   public static <MK, K, V> AtomicMap<K, V> getAtomicMap(Cache<MK, ?> cache, MK key, boolean createIfAbsent) {
      Object value = cache.get(key);
      if (value == null) {
         if (createIfAbsent)
            value = AtomicHashMap.newInstance(cache, key);
         else return null;
      }
      AtomicHashMap<K, V> castValue = (AtomicHashMap<K, V>) value;
      return castValue.getProxy(cache, key, cache.getAdvancedCache().getBatchContainer(), cache.getAdvancedCache().getInvocationContextContainer());
   }

   /**
    * Retrieves an atomic map from a given cache, stored under a given key, for reading only.  The atomic map returned
    * will not support updates, and if the map did not in fact exist, an empty map is returned.
    * @param cache underlying cache
    * @param key key under which the atomic map exists
    * @param <MK> key param of the cache
    * @param <K> key param of the AtomicMap
    * @param <V> value param of the AtomicMap
    * @return an immutable, read-only map
    */
   public static <MK, K, V> Map<K, V> getReadOnlyAtomicMap(Cache<MK, ?> cache, MK key) {
      AtomicMap<K, V> am = getAtomicMap(cache, key, false);
      if (am == null)
         return Collections.emptyMap();
      else
         return immutableMapWrap(am);
   }
}
