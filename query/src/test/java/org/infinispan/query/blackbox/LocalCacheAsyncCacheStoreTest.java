package org.infinispan.query.blackbox;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;

import org.infinispan.commons.util.Util;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Testing the ISPN Directory configuration with Async. FileCacheStore. The tests are performed on Local cache.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.LocalCacheAsyncCacheStoreTest")
public class LocalCacheAsyncCacheStoreTest extends LocalCacheTest {

   private final String indexDirectory = tmpDirectory("asyncStore");

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.fromXml("async-file-store-config.xml");
      cache = cacheManager.getCache("queryCache_lucenestore_async_filestore");
      return cacheManager;
   }

   @Override
   protected void setup() throws Exception {
      Util.recursiveFileRemove(indexDirectory);
      boolean created = new File(indexDirectory).mkdirs();
      assertTrue(created);
      super.setup();
   }

   @Override
   protected void teardown() {
      try {
         super.teardown();
      } finally {
         Util.recursiveFileRemove(indexDirectory);
      }
   }
}
