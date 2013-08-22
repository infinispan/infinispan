package org.infinispan.loaders.legacy;

import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.legacy.LegacyCacheStoreFunctionalTest")
public class LegacyCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @AfterClass
   protected void clearTempDir() {
      LegacyCacheStore.stores.remove(getClass().getName());
   }

   @Override
   protected LoadersConfigurationBuilder createCacheStoreConfig(LoadersConfigurationBuilder loaders) {
      loaders
         .addStore()
            .cacheStore(new LegacyCacheStore())
            .addProperty("storeName", getClass().getName())
            .purgeOnStartup(false)
            .purgeSynchronously(true);
      return loaders;
   }
}
