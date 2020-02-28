package org.infinispan.query.blackbox;

import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * Testing the query functionality on clustered caches started on TopologyAware nodes.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.TopologyAwareClusteredCacheTest")
public class TopologyAwareClusteredCacheTest extends ClusteredCacheTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      List<EmbeddedCacheManager> managers = TestQueryHelperFactory.createTopologyAwareCacheNodes(
               2, getCacheMode(), transactionEnabled(), isIndexLocalOnly(), isRamDirectory(), "default", Person.class);

      registerCacheManager(managers);

      cache1 = cache(0);
      cache2 = cache(1);

      waitForClusterToForm();
   }

   public CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   public boolean isIndexLocalOnly() {
      return false;
   }

   public boolean isRamDirectory() {
      return true;
   }

   public boolean transactionEnabled() {
      return false;
   }
}
