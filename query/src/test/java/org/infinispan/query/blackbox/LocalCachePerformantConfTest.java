package org.infinispan.query.blackbox;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;

import org.infinispan.commons.util.Util;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests the functionality of the queries in case when the NRT index manager is used in combination with FileStore.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.LocalCachePerformantConfTest")
public class LocalCachePerformantConfTest extends LocalCacheTest {

   /**
    * The file constant needs to match what's defined in the configuration file.
    */
   private final String indexDirectory = tmpDirectory("LocalCachePerformantConfTest");

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.fromXml("testconfig-LocalCachePerformantConfTest.xml");
      cache = cacheManager.getCache("Indexed");
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
