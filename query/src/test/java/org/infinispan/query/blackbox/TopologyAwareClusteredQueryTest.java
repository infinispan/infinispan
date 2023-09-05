package org.infinispan.query.blackbox;

import static org.infinispan.query.helper.TestQueryHelperFactory.createTopologyAwareCacheNodes;

import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
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
      List<EmbeddedCacheManager> managers = createTopologyAwareCacheNodes(2, getCacheMode(), transactionEnabled(),
                                                                          isIndexLocalOnly(), isRamDirectory(),
                                                                          "default", Person.class);

      registerCacheManager(managers);

      cacheAMachine1 = cache(0);
      cacheAMachine2 = cache(1);
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
