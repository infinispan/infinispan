package org.infinispan.loaders.dummy;

import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.dummy.DummyInMemoryCacheStoreFunctionalTest")
public class DummyInMemoryCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @AfterClass
   protected void clearTempDir() {
      DummyInMemoryCacheStore.stores.remove(getClass().getName());
   }

   @Override
   protected LoadersConfigurationBuilder createCacheStoreConfig(LoadersConfigurationBuilder loaders) {
      loaders
         .addStore(DummyInMemoryCacheStoreConfigurationBuilder.class)
            .storeName(getClass().getName())
            .purgeOnStartup(false)
            .purgeSynchronously(true);
      return loaders;
   }
}
