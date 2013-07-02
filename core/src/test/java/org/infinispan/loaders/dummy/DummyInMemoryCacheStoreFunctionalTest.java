package org.infinispan.loaders.dummy;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.CacheStoreConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.dummy.DummyInMemoryCacheStoreFunctionalTest")
public class DummyInMemoryCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @AfterClass
   protected void clearTempDir() {
      DummyInMemoryCacheStore.stores.remove(getClass().getName());
   }

   @Override
   protected CacheStoreConfig createCacheStoreConfig() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      DummyInMemoryCacheStoreConfigurationBuilder loader = builder.loaders().addLoader(DummyInMemoryCacheStoreConfigurationBuilder.class);
      loader.storeName(getClass().getName())
         .purgeOnStartup(false)
         .purgeSynchronously(true);
      return loader.create().adapt();
   }
}
