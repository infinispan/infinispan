package org.infinispan.tx.recovery;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "tx.recovery.RecoveryWithCustomCacheDistTest")
public class RecoveryWithCustomCacheDistTest extends RecoveryWithDefaultCacheDistTest {

   private static final String CUSTOM_CACHE = "customCache";

   private ConfigurationBuilder recoveryCache;

   @Override
   protected void createCacheManagers() throws Throwable {
      configuration = super.configure();
      configuration.transaction().recovery().recoveryInfoCacheName(CUSTOM_CACHE);

      registerCacheManager(TestCacheManagerFactory.createClusteredCacheManager(configuration));
      registerCacheManager(TestCacheManagerFactory.createClusteredCacheManager(configuration));

      recoveryCache = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      recoveryCache.transaction().transactionManagerLookup(null);
      // Explicitly disable recovery in recovery cache per se.
      recoveryCache.transaction().recovery().disable();
      manager(0).defineConfiguration(CUSTOM_CACHE, recoveryCache.build());
      manager(1).defineConfiguration(CUSTOM_CACHE, recoveryCache.build());

      manager(0).startCaches(getDefaultCacheName(), CUSTOM_CACHE);
      manager(1).startCaches(getDefaultCacheName(), CUSTOM_CACHE);
      waitForClusterToForm(CUSTOM_CACHE);

      assert manager(0).getCacheNames().contains(CUSTOM_CACHE);
      assert manager(1).getCacheNames().contains(CUSTOM_CACHE);
   }

   @Override
   protected String getRecoveryCacheName() {
      return CUSTOM_CACHE;
   }

   @Override
   protected void defineRecoveryCache(int cacheManagerIndex) {
      manager(1).defineConfiguration(CUSTOM_CACHE, recoveryCache.build());
   }
}
