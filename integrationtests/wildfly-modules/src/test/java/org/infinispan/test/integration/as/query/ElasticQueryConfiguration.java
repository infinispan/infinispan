package org.infinispan.test.integration.as.query;

import java.io.IOException;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * @since 9.0
 */
@ApplicationScoped
public class ElasticQueryConfiguration {

   @Produces
   @ApplicationScoped
   public Cache<String, Book> defaultClusteredCacheManager() throws IOException {
      return new DefaultCacheManager("elasticsearch-indexing.xml").getCache();
   }

   public void killCacheManager(@Disposes Cache<String, Book> cache) {
      EmbeddedCacheManager cacheManager = cache.getCacheManager();
      cache.stop();
      cacheManager.stop();
   }

}
