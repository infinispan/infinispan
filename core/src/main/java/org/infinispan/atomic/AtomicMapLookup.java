/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.atomic;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.context.FlagContainer;
import org.infinispan.util.InfinispanCollections;

import java.util.Map;

import static org.infinispan.util.Immutables.immutableMapWrap;

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
      return (AtomicMap<K, V>) getMap(cache, key, createIfAbsent, false, null);
   }

   /**
    * Retrieves an atomic map from a given cache, stored under a given key.
    *
    * @param cache          underlying cache
    * @param key            key under which the atomic map exists
    * @param flagContainer  a container to pass in per-invocation flags to the underlying cache.  May be null if no
    *                       flags are used.
    * @param <MK>           key param of the cache
    * @param <K>            key param of the AtomicMap
    * @param <V>            value param of the AtomicMap
    * @return an AtomicMap, or null if one did not exist.
    * @deprecated use {@link #getAtomicMap(org.infinispan.Cache, Object)}
    */
   @Deprecated
   public static <MK, K, V> AtomicMap<K, V> getAtomicMap(Cache<MK, ?> cache, MK key, FlagContainer flagContainer) {
      return (AtomicMap<K, V>) getMap(cache, key, true, false, flagContainer);
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
      return (FineGrainedAtomicMap<K, V>) getMap(cache, key, createIfAbsent, true, null);
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
    * @param flagContainer
    * @return an AtomicMap, or null if one did not exist.
    */
   @SuppressWarnings("unchecked")
   private static <MK, K, V> Map<K, V> getMap(Cache<MK, ?> cache, MK key, boolean createIfAbsent, boolean fineGrained, FlagContainer flagContainer) {
      Object value = cache.get(key);
      if (value == null) {
         if (createIfAbsent)
            value = AtomicHashMap.newInstance((Cache<Object,Object>) cache, key);
         else return null;
      }
      AtomicHashMap<K, V> castValue = (AtomicHashMap<K, V>) value;
      AtomicHashMapProxy<K, V> proxy =
            castValue.getProxy((AdvancedCache<Object,Object>) cache.getAdvancedCache(), key, fineGrained, flagContainer);
      boolean typeSwitchAttempt = proxy instanceof FineGrainedAtomicHashMapProxy != fineGrained;
      if (typeSwitchAttempt) {
         throw new IllegalArgumentException("Cannot switch type of previously used " + value
                                                  + " from " + (fineGrained ? "regular to fine-grained!" : "fine-grained to regular!"));
      }
      return proxy;
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
         return InfinispanCollections.emptyMap();
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
      cache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES).remove(key);
   }
}
