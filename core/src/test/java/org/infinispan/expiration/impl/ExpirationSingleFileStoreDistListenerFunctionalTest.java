package org.infinispan.expiration.impl;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationSingleFileStoreDistListenerFunctionalTest")
public class ExpirationSingleFileStoreDistListenerFunctionalTest extends ExpirationStoreListenerFunctionalTest {
   @Override
   protected void configure(ConfigurationBuilder config) {
      config
              // Prevent the reaper from running, reaperEnabled(false) doesn't work when a store is present
              .expiration().wakeUpInterval(Long.MAX_VALUE)
              .clustering().cacheMode(CacheMode.DIST_SYNC)
              .persistence().addSingleFileStore();
   }

   @Override
   protected EmbeddedCacheManager createCacheManager(ConfigurationBuilder builder) {
      return TestCacheManagerFactory.createClusteredCacheManager(builder);
   }
}
