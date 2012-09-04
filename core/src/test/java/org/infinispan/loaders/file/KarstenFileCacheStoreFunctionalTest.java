package org.infinispan.loaders.file;

import org.infinispan.Cache;
import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;

import static org.infinispan.test.TestingUtil.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "unit", testName = "loaders.file.KarstenFileCacheStoreFunctionalTest")
public class KarstenFileCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   private String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   @Override
   protected CacheStoreConfig createCacheStoreConfig() throws Exception {
      KarstenFileCacheStoreConfig cfg = new KarstenFileCacheStoreConfig();
      cfg.setLocation(tmpDirectory);
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      return cfg;
   }

   public void testPassivationOnDefaultEvictionOnNamed() throws Exception {
      String config = INFINISPAN_START_TAG_NO_SCHEMA +
            "<default>\n" +
            "<loaders passivation=\"false\" shared=\"false\" preload=\"true\"> \n" +
            "<loader class=\"org.infinispan.loaders.file.KarstenFileCacheStore\" \n" +
            "fetchPersistentState=\"false\" purgerThreads=\"3\" purgeSynchronously=\"true\" \n" +
            "ignoreModifications=\"false\" purgeOnStartup=\"false\"> \n" +
            "</loader>\n" +
            "</loaders>\n" +
            "</default>\n" + INFINISPAN_END_TAG;
      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache();
            cache.put(1, "v1");
            assertEquals("v1", cache.get(1));
            CacheStore store = extractComponent(cache, CacheLoaderManager.class).getCacheStore();
            assertTrue(store instanceof KarstenFileCacheStore);
         }
      });
   }


}
