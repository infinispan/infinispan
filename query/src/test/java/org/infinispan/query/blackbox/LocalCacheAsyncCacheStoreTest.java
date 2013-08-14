package org.infinispan.query.blackbox;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Testing the ISPN Directory configuration with Async. FileCacheStore. The tests are performed on Local cache.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.LocalCacheAsyncCacheStoreTest")
public class LocalCacheAsyncCacheStoreTest extends LocalCacheTest {

   private String indexDirectory = System.getProperty("java.io.tmpdir") + File.separator + "asyncStore";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.fromXml("async-store-config.xml");
      cache = cacheManager.getCache("queryCache_lucenestore_async_filestore");

      return cacheManager;
   }

   @Override
   protected void setup() throws Exception {
      new File(indexDirectory).mkdirs();
      super.setup();
   }

   @Override
   protected void teardown() {
      try {
         super.teardown();
      } finally {
         TestingUtil.recursiveFileRemove(indexDirectory);
      }
   }
}
