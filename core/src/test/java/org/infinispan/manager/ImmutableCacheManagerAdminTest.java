package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(testName = "manager.ImmutableCacheManagerAdminTest", groups = "functional")
@CleanupAfterMethod
public class ImmutableCacheManagerAdminTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().configurationStorage(ConfigurationStorage.IMMUTABLE);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);
   }

   public void testClusterCacheTest() {
      waitForClusterToForm();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      Configuration configuration = builder.build();
      Cache<Object, Object> cache = manager(0).administration().createCache("a", configuration);

   }
}
