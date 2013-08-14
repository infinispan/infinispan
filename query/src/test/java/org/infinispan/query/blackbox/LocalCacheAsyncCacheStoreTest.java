package org.infinispan.query.blackbox;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Testing the ISPN Directory configuration with Async. FileCacheStore. The tests are performed on Local cache.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.LocalCacheAsyncCacheStoreTest")
public class LocalCacheAsyncCacheStoreTest extends LocalCacheTest {

   private String indexDirector;
   private String directoryName = "/asyncStore";

   public LocalCacheAsyncCacheStoreTest() {
      indexDirector = System.getProperty("java.io.tmpdir") + directoryName;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.fromXml("async-store-config.xml");
      cache = cacheManager.getCache("queryCache_lucenestore_async_filestore");

      return cacheManager;
   }

   @BeforeMethod
   protected void createDirectory() {
      new File(indexDirector).mkdirs();
   }

   @Override
   @AfterMethod
   protected void destroyAfterMethod() {
      try {
         //first stop cache managers, then clear the index
         super.destroyAfterMethod();
      } finally {
         //delete the index otherwise it will mess up the index for next tests
         TestingUtil.recursiveFileRemove(indexDirector);
      }
   }
}
