package org.infinispan.multimap.impl;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.api.embedded.MultimapCache;
import org.infinispan.multimap.api.embedded.MultimapCacheManager;

/**
 * Embedded implementation of {@link MultimapCacheManager}
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class EmbeddedMultimapCacheManager implements MultimapCacheManager {

   private final EmbeddedCacheManager cacheManager;

   public EmbeddedMultimapCacheManager(EmbeddedCacheManager embeddedMultimapCacheManager) {
      this.cacheManager = embeddedMultimapCacheManager;
   }

   @Override
   public Configuration defineConfiguration(String name, Configuration configuration) {
      return cacheManager.defineConfiguration(name, configuration);
   }

   @Override
   public <K, V> MultimapCache<K, V> get(String name, boolean supportsDuplicates) {
      Cache<K, Bucket<V>> cache = cacheManager.getCache(name, true);
      return new EmbeddedMultimapCache<>(cache, supportsDuplicates);
   }

   /**
    * Provides an api to manipulate key/values with lists.
    *
    * @param cacheName, name of the cache
    * @return EmbeddedMultimapListCache
    */
   public <K, V> EmbeddedMultimapListCache<K, V> getMultimapList(String cacheName) {
      Cache<K, ListBucket<V>> cache = cacheManager.getCache(cacheName);
      if (cache == null) {
         throw new IllegalStateException("Cache must exist: " + cacheName);
      }
      return new EmbeddedMultimapListCache<>(cache);
   }

   /**
    * Provides an api to manipulate key/values with sorted sets.
    *
    * @param cacheName, name of the cache
    * @return EmbeddedMultimapSortedSetCache
    */
   public <K, V>EmbeddedMultimapSortedSetCache<K, V> getMultimapSortedSet(String cacheName) {
      Cache<K, SortedSetBucket<V>> cache = cacheManager.getCache(cacheName);
      if (cache == null) {
         throw new IllegalStateException("Cache must exist: " + cacheName);
      }
      return new EmbeddedMultimapSortedSetCache<>(cache);
   }

   public <K, HK, HV> EmbeddedMultimapPairCache<K, HK, HV> getMultimapPair(String cacheName) {
      Cache<K, HashMapBucket<HK, HV>> cache = cacheManager.getCache(cacheName);
      if (cache == null) {
         throw new IllegalStateException("Cache must exist: " + cacheName);
      }
      return new EmbeddedMultimapPairCache<>(cache);
   }
}
