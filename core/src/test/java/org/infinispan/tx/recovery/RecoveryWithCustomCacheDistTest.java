package org.infinispan.tx.recovery;

import org.infinispan.config.Configuration;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "tx.recovery.RecoveryWithCustomCacheDistTest")
public class RecoveryWithCustomCacheDistTest extends RecoveryWithDefaultCacheDistTest {

   private static final String CUSTOM_CACHE = "customCache";
   private Configuration recoveryCache;

   @Override
   protected void createCacheManagers() throws Throwable {
      configuration = super.configure();
      configuration.fluent().transaction().recovery().recoveryInfoCacheName(CUSTOM_CACHE);

      registerCacheManager(TestCacheManagerFactory.createClusteredCacheManager(configuration, false));
      registerCacheManager(TestCacheManagerFactory.createClusteredCacheManager(configuration, false));

      recoveryCache = getDefaultClusteredConfig(Configuration.CacheMode.LOCAL);
      // Explicitly disable recovery in recovery cache per se.
      recoveryCache.fluent().transaction().recovery().disable();
      manager(0).defineConfiguration(CUSTOM_CACHE, recoveryCache);
      manager(1).defineConfiguration(CUSTOM_CACHE, recoveryCache);

      cache(0);
      cache(1);


      waitForClusterToForm();

      assert manager(0).getCacheNames().contains(CUSTOM_CACHE);
      assert manager(1).getCacheNames().contains(CUSTOM_CACHE);
   }

   @Override
   protected String getRecoveryCacheName() {
      return CUSTOM_CACHE;
   }

   @Override
   protected void defineRecoveryCache(int cacheManagerIndex) {
      manager(1).defineConfiguration(CUSTOM_CACHE, recoveryCache);
   }
}
