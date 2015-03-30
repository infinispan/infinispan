package org.infinispan.jcache;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

import static org.infinispan.test.AbstractCacheTest.getDefaultClusteredCacheConfig;

public class CacheProducer {

   @Produces
   @ApplicationScoped
   public EmbeddedCacheManager defaultClusteredCacheManager() {
      ConfigurationBuilder defaultClusteredCacheConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(defaultClusteredCacheConfig);
      cacheManager.defineConfiguration("annotation", getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC).build());
      return cacheManager;
   }

   public void destroy(@Disposes EmbeddedCacheManager cacheManager) {
      TestingUtil.killCacheManagers(cacheManager);
   }
}