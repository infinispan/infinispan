package org.infinispan.query.blackbox;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Tests the functionality of the queries in case when the NRT index manager is used in combination with FileStore.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.LocalCachePerformantConfTest")
public class LocalCachePerformantConfTest extends LocalCacheTest {

   private final String indexDirectory = System.getProperty("java.io.tmpdir") + File.separator + "tunedConfDir";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.fromXml("nrt-performance-writer.xml");
      cache = cacheManager.getCache("Indexed");

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
