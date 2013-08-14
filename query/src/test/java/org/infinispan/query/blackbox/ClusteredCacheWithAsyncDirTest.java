package org.infinispan.query.blackbox;

import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Testing the ISPN Directory configuration with Async. JDBC CacheStore. The tests are performed for Clustered cache.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheWithAsyncDirTest")
public class ClusteredCacheWithAsyncDirTest extends ClusteredCacheTest {

   @Override
   protected void createCacheManagers() throws Exception {
      cacheManagers.add(TestCacheManagerFactory.fromXml("async-store-config.xml"));
      cacheManagers.add(TestCacheManagerFactory.fromXml("async-store-config.xml"));
      waitForClusterToForm();

      cache1 = cacheManagers.get(0).getCache("JDBCBased_LocalIndex");
      cache2 = cacheManagers.get(1).getCache("JDBCBased_LocalIndex");
   }

   protected boolean transactionsEnabled() {
      return true;
   }
}
