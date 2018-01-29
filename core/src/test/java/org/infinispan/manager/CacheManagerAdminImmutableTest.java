package org.infinispan.manager;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(testName = "manager.CacheManagerAdminImmutableTest", groups = "functional")
@CleanupAfterMethod
public class CacheManagerAdminImmutableTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < 2; i++) {
         GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
         global.globalState().configurationStorage(ConfigurationStorage.IMMUTABLE);
         ConfigurationBuilder builder = new ConfigurationBuilder();
         builder.clustering().cacheMode(CacheMode.DIST_SYNC);
         addClusterEnabledCacheManager(global, builder);
      }
   }

   public void testClusterCacheTest() {
      waitForClusterToForm();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      Configuration configuration = builder.build();

      Exceptions.expectException(UnsupportedOperationException.class,
            "ISPN000515: The configuration is immutable",
            () -> manager(0).administration().createCache("a", configuration));

      // attempt to delete a cache
      Exceptions.expectException(UnsupportedOperationException .class,
            "ISPN000515: The configuration is immutable",
            () -> manager(0).administration().removeCache("a"));
   }
}
