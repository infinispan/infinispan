package org.horizon.loader.dummy;

import org.horizon.loader.BaseCacheStoreTest;
import org.horizon.loader.CacheStore;
import org.horizon.loader.CacheLoaderException;
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
