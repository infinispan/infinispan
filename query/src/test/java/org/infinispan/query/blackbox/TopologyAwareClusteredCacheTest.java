package org.infinispan.query.blackbox;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
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
      List caches = TestQueryHelperFactory.createTopologyAwareCacheNodes(
               2, getCacheMode(), transactionEnabled(), isIndexLocalOnly(), isRamDirectory(), "default");

      for (Object cache : caches) {
         cacheManagers.add(((Cache) cache).getCacheManager());
      }

      cache1 = (Cache<String, Person>) caches.get(0);
      cache2 = (Cache<String, Person>) caches.get(1);

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
