package org.infinispan.loaders.dummy;

import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.dummy.DummyInMemoryCacheStoreTest")
public class DummyInMemoryCacheStoreTest extends BaseCacheStoreTest {

   protected CacheStore createCacheStore() throws CacheLoaderException {
      DummyInMemoryCacheStore cl = new DummyInMemoryCacheStore();
      DummyInMemoryCacheStore.Cfg cfg = new DummyInMemoryCacheStore.Cfg();
      cfg.setStore(DummyInMemoryCacheStoreTest.class.getName());
      cl.init(cfg, getCache(), getMarshaller());
      cl.start();
      return cl;
   }
}
