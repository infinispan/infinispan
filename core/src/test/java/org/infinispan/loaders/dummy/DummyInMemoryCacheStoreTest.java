package org.infinispan.loaders.dummy;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.dummy.DummyInMemoryCacheStoreTest")
public class DummyInMemoryCacheStoreTest extends BaseCacheStoreTest {

   @Override
   protected CacheStore createCacheStore() throws CacheLoaderException {
      DummyInMemoryCacheStore cl = new DummyInMemoryCacheStore();
      ConfigurationBuilder builder = new ConfigurationBuilder();
      DummyInMemoryCacheStoreConfigurationBuilder loader = builder.loaders().addLoader(DummyInMemoryCacheStoreConfigurationBuilder.class);
      loader
         .storeName(getClass().getName())
         .purgeSynchronously(true);
      cl.init( loader.create().adapt(), getCache(), getMarshaller());
      cl.start();
      return cl;
   }
}
