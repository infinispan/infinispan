package org.infinispan.query.blackbox;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * Tests for testing clustered queries functionality on topology aware nodes.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.TopologyAwareClusteredQueryTest")
public class TopologyAwareClusteredQueryTest extends ClusteredQueryTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      List caches = TestQueryHelperFactory.createTopologyAwareCacheNodes(2, getCacheMode(), transactionEnabled(),
            isIndexLocalOnly(), isRamDirectory(), "default", Person.class);

      for (Object cache : caches) {
         cacheManagers.add(((Cache) cache).getCacheManager());
      }

      cacheAMachine1 = (Cache<String, Person>) caches.get(0);
      cacheAMachine2 = (Cache<String, Person>) caches.get(1);

      waitForClusterToForm();
      populateCache();
   }

   public CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   public boolean isIndexLocalOnly() {
      return true;
   }

   public boolean isRamDirectory() {
      return true;
   }

   public boolean transactionEnabled() {
      return false;
   }
}
