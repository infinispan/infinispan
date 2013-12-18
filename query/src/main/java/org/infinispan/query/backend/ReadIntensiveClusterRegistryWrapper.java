package org.infinispan.query.backend;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.registry.ClusterRegistry;

/**
 * Decorates a ClusterRegistry to provide a cache for read operations.
 * Write operations are expected to happen only exceptionally, therefore this code
 * is heavily optimized for reads (at cost of writes).
 * Also we're assuming all entries are small: there is no size limit nor cleanup strategy.
 *
 * This is not caching the fact that some key is not defined: that would be tricky to
 * get right and is not needed for our use case.
 *
 * @author Sanne Grinovero (C) 2013 Red Hat Inc.
 */
@ThreadSafe
final class ReadIntensiveClusterRegistryWrapper<S,K,V> {

   private final ClusterRegistry<S, K, V> clusterRegistry;
   private final S scope;

   /**
    * Not using a ConcurrentHashMap as this will degenerate into a read-only Map at runtime;
    * in the Query specific case we're only adding new class types while they are being discovered,
    * after this initial phase this is supposed to be a read-only immutable map.
    */
   private final AtomicReference<Map<K,V>> localCache = new AtomicReference<Map<K,V>>(Collections.EMPTY_MAP);

   ReadIntensiveClusterRegistryWrapper(ClusterRegistry<S, K, V> clusterRegistry, S scope) {
      this.clusterRegistry = clusterRegistry;
      this.scope = scope;
   }

   void addListener(final Object registryListener) {
      clusterRegistry.addListener(scope, registryListener);
   }

   Set<K> keys() {
      return clusterRegistry.keys(scope);
   }

   void removeListener(final Object registryListener) {
      clusterRegistry.removeListener(registryListener);
   }

   boolean containsKey(final K key) {
      final boolean localExists = localCache.get().containsKey(key);
      if (localExists) {
         return true;
      }
      else {
         V fetchedValue = get(key);//possibly load it into local cache
         return fetchedValue != null;
      }
   }

   void put(final K key, final V value) {
      clusterRegistry.put(scope, key, value);
      localCacheInsert(key, value);
   }

   V get(final K key) {
      V v = localCache.get().get(key);
      if (v!=null) {
         return v;
      }
      else {
         v = clusterRegistry.get(scope, key);
         if (v!=null) {
            localCacheInsert(key, v);
         }
         return v;
      }
   }

   private void localCacheInsert(final K key, final V value) {
      synchronized (localCache) {
         final Map<K, V> currentContent = localCache.get();
         final int currentSize = currentContent.size();
         if (currentSize==0) {
            localCache.lazySet(Collections.singletonMap(key, value));
         }
         else {
            Map<K,V> updatedContent = new HashMap<K,V>(currentSize+1);
            updatedContent.putAll(currentContent);
            updatedContent.put(key, value);
            localCache.lazySet(Collections.unmodifiableMap(updatedContent));
         }
      }
   }

}
