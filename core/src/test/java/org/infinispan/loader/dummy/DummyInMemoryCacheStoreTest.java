package org.infinispan.loader.dummy;

import org.infinispan.loader.BaseCacheStoreTest;
import org.infinispan.loader.CacheStore;
import org.infinispan.loader.CacheLoaderException;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loader.dummy.DummyInMemoryCacheStoreTest")
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
