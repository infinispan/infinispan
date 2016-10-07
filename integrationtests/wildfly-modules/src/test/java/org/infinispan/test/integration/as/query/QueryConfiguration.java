package org.infinispan.test.integration.as.query;

import java.io.IOException;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Producer for a CacheManager able to use distributed Query
 *
 * @author Sanne Grinovero
 */
@ApplicationScoped
public class QueryConfiguration {

   @Produces @ApplicationScoped
   public Cache<String,Book> defaultClusteredCacheManager() throws IOException {
      DefaultCacheManager cacheManager = new DefaultCacheManager("dynamic-indexing-distribution.xml");
      Cache<String, Book> cache = cacheManager.getCache();
      return cache;
   }

   public void killCacheManager(@Disposes Cache<String, Book> cache) {
      EmbeddedCacheManager cacheManager = cache.getCacheManager();
      cache.stop();
      cacheManager.stop();
   }

}
