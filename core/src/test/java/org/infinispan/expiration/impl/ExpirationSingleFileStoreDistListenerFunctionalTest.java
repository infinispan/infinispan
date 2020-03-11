package org.infinispan.expiration.impl;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationSingleFileStoreDistListenerFunctionalTest")
public class ExpirationSingleFileStoreDistListenerFunctionalTest extends ExpirationStoreListenerFunctionalTest {
   private EmbeddedCacheManager extraManager;
   private Cache<Object, Object> extraCache;

   @Factory
   @Override
   public Object[] factory() {
      return new Object[]{
            // Test is for single file store with a listener in a dist cache and we don't care about memory storage types
            new ExpirationSingleFileStoreDistListenerFunctionalTest().cacheMode(CacheMode.DIST_SYNC),
      };
   }

   @Override
   protected void configure(ConfigurationBuilder config) {
      config
              // Prevent the reaper from running, reaperEnabled(false) doesn't work when a store is present
              .expiration().wakeUpInterval(Long.MAX_VALUE)
              .clustering().cacheMode(cacheMode)
              .persistence().addSingleFileStore().location(tmpDirectory(this.getClass()));
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory(this.getClass()));
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName() + "2"));
   }

   protected void removeFromContainer(String key) {
      super.removeFromContainer(key);
      extraCache.getAdvancedCache().getDataContainer().remove(key);
   }

   @Override
   protected void processExpiration() {
      // Invoking process expiration only removes primary owned entries - so we need to invoke it on the extra cache
      // in addition to the original cache
      super.processExpiration();
      TestingUtil.extractComponent(extraCache, InternalExpirationManager.class).processExpiration();
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() {
      super.clearContent();
      // We also have to clear the content of the extraManager
      TestingUtil.clearContent(extraManager);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager(ConfigurationBuilder builder) {
      extraManager = createClusteredCacheManager(false, GlobalConfigurationBuilder.defaultClusteredBuilder(),
                                                 builder, new TransportFlags());
      // Inject our time service into the new CacheManager as well
      TestingUtil.replaceComponent(extraManager, TimeService.class, timeService, true);
      extraCache = extraManager.getCache();
      SingleFileStoreConfigurationBuilder sfsBuilder =
            (SingleFileStoreConfigurationBuilder) builder.persistence().stores().get(0);
      // Make sure each cache writes to a different location
      sfsBuilder.location(tmpDirectory(this.getClass().getSimpleName() + "2"));
      EmbeddedCacheManager returned =
            createClusteredCacheManager(false, GlobalConfigurationBuilder.defaultClusteredBuilder(),
                                        builder, new TransportFlags());
      // Unfortunately we can't reinject timeservice once a cache has been started, thus we have to inject
      // here as well, since we need the cache to verify the cluster was formed
      TestingUtil.replaceComponent(returned, TimeService.class, timeService, true);
      Cache<Object, Object> checkCache = returned.getCache();
      TestingUtil.blockUntilViewReceived(checkCache, 2, TimeUnit.SECONDS.toMillis(10));
      return returned;
   }
}
