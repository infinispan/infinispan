package org.infinispan.tx.recovery;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "tx.recovery.RecoveryWithCustomCacheDistTest")
public class RecoveryWithCustomCacheDistTest extends RecoveryWithDefaultCacheDistTest {

   private static final String CUSTOM_CACHE = "customCache";

   private ConfigurationBuilder recoveryCacheConfig;

   @Override
   protected void createCacheManagers() throws Throwable {
      configuration = super.configure();
      configuration.transaction().recovery().recoveryInfoCacheName(CUSTOM_CACHE);

      recoveryCacheConfig = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      recoveryCacheConfig.transaction().transactionManagerLookup(null);
      // Explicitly disable recovery in recovery cache per se.
      recoveryCacheConfig.transaction().recovery().disable();

      startCacheManager();
      startCacheManager();

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
   protected void startCacheManager() {
      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      holder.getGlobalConfigurationBuilder().clusteredDefault().defaultCacheName(getDefaultCacheName());
      holder.getNamedConfigurationBuilders().put(getDefaultCacheName(), configuration);
      holder.getNamedConfigurationBuilders().put(CUSTOM_CACHE, recoveryCacheConfig);

      registerCacheManager(TestCacheManagerFactory.createClusteredCacheManager(holder));
   }
}
