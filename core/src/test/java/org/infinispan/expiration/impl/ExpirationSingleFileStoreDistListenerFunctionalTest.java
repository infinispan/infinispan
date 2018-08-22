package org.infinispan.expiration.impl;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.commons.time.TimeService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationSingleFileStoreDistListenerFunctionalTest")
public class ExpirationSingleFileStoreDistListenerFunctionalTest extends ExpirationStoreListenerFunctionalTest {
   private EmbeddedCacheManager extraManager;
   private Cache<Object, Object> extraCache;

   @Override
   protected void configure(ConfigurationBuilder config) {
      config
              // Prevent the reaper from running, reaperEnabled(false) doesn't work when a store is present
              .expiration().wakeUpInterval(Long.MAX_VALUE)
              .clustering().cacheMode(CacheMode.DIST_SYNC)
              .persistence().addSingleFileStore().location(TestingUtil.tmpDirectory(this.getClass()));
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(TestingUtil.tmpDirectory(this.getClass()));
      Util.recursiveFileRemove(TestingUtil.tmpDirectory(this.getClass().getSimpleName() + "2"));
   }

   protected void removeFromContainer(String key) {
      super.removeFromContainer(key);
      extraCache.getAdvancedCache().getDataContainer().remove(key);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager(ConfigurationBuilder builder) {
      extraManager = TestCacheManagerFactory.createClusteredCacheManager(builder);
      // Inject our time service into the new CacheManager as well
      TestingUtil.replaceComponent(extraManager, TimeService.class, timeService, true);
      extraCache = extraManager.getCache();
      SingleFileStoreConfigurationBuilder sfsBuilder = (SingleFileStoreConfigurationBuilder) builder.persistence().stores().get(0);
      // Make sure each cache writes to a different location
      sfsBuilder.location(TestingUtil.tmpDirectory(this.getClass().getSimpleName() + "2"));
      EmbeddedCacheManager returned = TestCacheManagerFactory.createClusteredCacheManager(builder);
      // Unfortunately we can't reinject timeservice once a cache has been started, thus we have to inject
      // here as well, since we need the cache to verify the cluster was formed
      TestingUtil.replaceComponent(returned, TimeService.class, timeService, true);
      Cache<Object, Object> checkCache = returned.getCache();
      TestingUtil.blockUntilViewReceived(checkCache, 2, TimeUnit.SECONDS.toMillis(10));
      return returned;
   }
}
