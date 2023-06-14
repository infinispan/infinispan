package org.infinispan.multimap.impl;

import java.util.Collection;

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
public class EmbeddedMultimapCacheManager<K, V> implements MultimapCacheManager<K, V> {

   private EmbeddedCacheManager cacheManager;

   public EmbeddedMultimapCacheManager(EmbeddedCacheManager embeddedMultimapCacheManager) {
      this.cacheManager = embeddedMultimapCacheManager;
   }

   @Override
   public Configuration defineConfiguration(String name, Configuration configuration) {
      return cacheManager.defineConfiguration(name, configuration);
   }

   @Override
   public MultimapCache<K, V> get(String name, boolean supportsDuplicates) {
      Cache<K, Collection<V>> cache = cacheManager.getCache(name, true);
      EmbeddedMultimapCache multimapCache = new EmbeddedMultimapCache(cache, supportsDuplicates);
      return multimapCache;
   }

   /**
    * Provides an api to manipulate key/values with lists.
    *
    * @param cacheName, name of the cache
    * @return EmbeddedMultimapListCache
    */
   public EmbeddedMultimapListCache<K, V> getMultimapList(String cacheName) {
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
   public EmbeddedMultimapSortedSetCache<K, V> getMultimapSortedSet(String cacheName) {
      Cache<K, SortedSetBucket<V>> cache = cacheManager.getCache(cacheName);
      if (cache == null) {
         throw new IllegalStateException("Cache must exist: " + cacheName);
      }
      return new EmbeddedMultimapSortedSetCache<>(cache);
   }

   public <HK, HV> EmbeddedMultimapPairCache<K, HK, HV> getMultimapPair(String cacheName) {
      Cache<K, HashMapBucket<HK, HV>> cache = cacheManager.getCache(cacheName);
      if (cache == null) {
         throw new IllegalStateException("Cache must exist: " + cacheName);
      }
      return new EmbeddedMultimapPairCache<>(cache);
   }
}
