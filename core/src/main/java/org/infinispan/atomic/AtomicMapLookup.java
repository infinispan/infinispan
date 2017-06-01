package org.infinispan.atomic;

import static org.infinispan.commons.util.Immutables.immutableMapWrap;

import java.util.Collections;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.atomic.impl.AtomicMapProxyImpl;
import org.infinispan.atomic.impl.FineGrainedAtomicMapProxyImpl;

/**
 * A helper that locates or safely constructs and registers atomic maps with a given cache.  This should be the
 * <b>only</b> way AtomicMaps are created/retrieved, to prevent concurrent creation, registration and possibly
 * overwriting of such a map within the cache.
 *
 * @author Manik Surtani
 * @see AtomicMap
 * @since 4.0
 */
public class AtomicMapLookup {
   /**
    * Retrieves an atomic map from a given cache, stored under a given key.  If an atomic map did not exist, one is
    * created and registered in an atomic fashion.
    *
    * @param cache underlying cache
    * @param key   key under which the atomic map exists
    * @param <MK>  key param of the cache
    * @param <K>   key param of the AtomicMap
    * @param <V>   value param of the AtomicMap
    * @return an AtomicMap
    */
   public static <MK, K, V> AtomicMap<K, V> getAtomicMap(Cache<MK, ?> cache, MK key) {
      return getAtomicMap(cache, key, true);
   }

   /**
    * Retrieves a fine grained atomic map from a given cache, stored under a given key. If a fine grained atomic map did
    * not exist, one is created and registered in an atomic fashion.
    *
    * @param cache underlying cache
    * @param key   key under which the atomic map exists
    * @param <MK>  key param of the cache
    * @param <K>   key param of the AtomicMap
    * @param <V>   value param of the AtomicMap
    * @return an AtomicMap
    */
   public static <MK, K, V> FineGrainedAtomicMap<K, V> getFineGrainedAtomicMap(Cache<MK, ?> cache, MK key) {
      return getFineGrainedAtomicMap(cache, key, true);
   }

   /**
    * Retrieves an atomic map from a given cache, stored under a given key.
    *
    * @param cache          underlying cache
    * @param key            key under which the atomic map exists
    * @param createIfAbsent if true, a new atomic map is created if one doesn't exist; otherwise null is returned if the
    *                       map didn't exist.
    * @param <MK>           key param of the cache
    * @param <K>            key param of the AtomicMap
    * @param <V>            value param of the AtomicMap
    * @return an AtomicMap, or null if one did not exist.
    */
   public static <MK, K, V> AtomicMap<K, V> getAtomicMap(Cache<MK, ?> cache, MK key, boolean createIfAbsent) {
      return (AtomicMap<K, V>) getMap((Cache<MK, Object>) cache, key, createIfAbsent, false);
   }

   /**
    * Retrieves an atomic map from a given cache, stored under a given key.
    *
    * @param cache          underlying cache
    * @param key            key under which the atomic map exists
    * @param createIfAbsent if true, a new atomic map is created if one doesn't exist; otherwise null is returned if the
    *                       map didn't exist.
    * @param <MK>           key param of the cache
    * @param <K>            key param of the AtomicMap
    * @param <V>            value param of the AtomicMap
    * @return an AtomicMap, or null if one did not exist.
    */
   public static <MK, K, V> FineGrainedAtomicMap<K, V> getFineGrainedAtomicMap(Cache<MK, ?> cache, MK key, boolean createIfAbsent) {
      return (FineGrainedAtomicMap<K, V>) getMap((Cache<MK, Object>) cache, key, createIfAbsent, true);
   }

   /**
    * Retrieves an atomic map from a given cache, stored under a given key.
    *
    *
    * @param cache          underlying cache
    * @param key            key under which the atomic map exists
    * @param createIfAbsent if true, a new atomic map is created if one doesn't exist; otherwise null is returned if the
    *                       map didn't exist.
    * @param fineGrained    if true, and createIfAbsent is true then created atomic map will be fine grained.
    * @return an AtomicMap, or null if one did not exist.
    */
   @SuppressWarnings("unchecked")
   private static <MK, K, V> Map<K, V> getMap(Cache<MK, Object> cache, MK key, boolean createIfAbsent, boolean fineGrained) {
      if (fineGrained) {
         // With fine grained maps the cache will store both the keyset under master key and the entries under group keys
         return FineGrainedAtomicMapProxyImpl.newInstance((Cache<Object, Object>) cache, key, createIfAbsent);
      } else {
         return AtomicMapProxyImpl.newInstance(cache, key, createIfAbsent);
      }
   }

   /**
    * Retrieves an atomic map from a given cache, stored under a given key, for reading only.  The atomic map returned
    * will not support updates, and if the map did not in fact exist, an empty map is returned.
    *
    * @param cache underlying cache
    * @param key   key under which the atomic map exists
    * @param <MK>  key param of the cache
    * @param <K>   key param of the AtomicMap
    * @param <V>   value param of the AtomicMap
    * @return an immutable, read-only map
    */
   public static <MK, K, V> Map<K, V> getReadOnlyAtomicMap(Cache<MK, ?> cache, MK key) {
      AtomicMap<K, V> am = getAtomicMap(cache, key, false);
      if (am == null)
         return Collections.emptyMap();
      else
         return immutableMapWrap(am);
   }

   /**
    * Removes the atomic map associated with the given key from the underlying cache.
    *
    * @param cache underlying cache
    * @param key   key under which the atomic map exists
    * @param <MK>  key param of the cache
    */
   public static <MK> void removeAtomicMap(Cache<MK, ?> cache, MK key) {
      // This removes any entry from the cache but includes special handling for fine-grained maps
      FineGrainedAtomicMapProxyImpl.removeMap((Cache<Object, Object>) cache, key);
   }
}
