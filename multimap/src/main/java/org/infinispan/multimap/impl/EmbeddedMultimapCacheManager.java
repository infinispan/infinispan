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

   public EmbeddedMultimapCacheManager(EmbeddedCacheManager embeddedMultimapCacheManager){
      this.cacheManager = embeddedMultimapCacheManager;
   }

   @Override
   public Configuration defineConfiguration(String name, Configuration configuration) {
      return cacheManager.defineConfiguration(name, configuration);
   }

   @Override
   public MultimapCache<K, V> get(String name) {
      Cache<K, Collection<V>> cache = cacheManager.getCache(name, true);
      EmbeddedMultimapCache multimapCache = new EmbeddedMultimapCache(cache);
      return multimapCache;
   }
}
