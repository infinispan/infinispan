package org.infinispan.query.blackbox;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests the functionality of the queries in case when the NRT index manager is used in combination with ISPN Directory.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.LocalCachePerfIspnDirConfTest")
public class LocalCachePerfIspnDirConfTest extends LocalCacheTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.fromXml("nrt-performance-writer-infinispandirectory.xml");
      cache = cacheManager.getCache("Indexed");

      return cacheManager;
   }
}
