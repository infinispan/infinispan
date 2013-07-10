package org.infinispan.loaders.dummy;

import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.dummy.DummyInMemoryCacheStoreTest")
public class DummyInMemoryCacheStoreTest extends BaseCacheStoreTest {

   @Override
   protected CacheStore createCacheStore() throws CacheLoaderException {
      DummyInMemoryCacheStore cl = new DummyInMemoryCacheStore();
      DummyInMemoryCacheStoreConfigurationBuilder loader = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .loaders()
            .addLoader(DummyInMemoryCacheStoreConfigurationBuilder.class);
      loader
         .storeName(getClass().getName())
         .purgeSynchronously(true);
      cl.init(loader.create(), getCache(), getMarshaller());
      cl.start();
      if (cl.getConfiguration() == null) throw new NullPointerException();
      return cl;
   }
}
