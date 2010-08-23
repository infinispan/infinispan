package org.infinispan.loaders.dummy;

import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.file.FileCacheStoreConfig;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;

@Test(groups = "unit", testName = "loaders.dummy.DummyInMemoryCacheStoreFunctionalTest")
public class DummyInMemoryCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @AfterClass
   protected void clearTempDir() {
      DummyInMemoryCacheStore.stores.remove(getClass().getName());
   }

   @Override
   protected CacheStoreConfig createCacheStoreConfig() throws Exception {
      DummyInMemoryCacheStore.Cfg cfg = new DummyInMemoryCacheStore.Cfg(getClass().getName(), false);
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      return cfg;
   }

}
