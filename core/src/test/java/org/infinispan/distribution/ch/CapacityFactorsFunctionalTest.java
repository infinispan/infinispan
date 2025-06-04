package org.infinispan.distribution.ch;

import static org.infinispan.test.TestingUtil.assertBetween;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Map;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.impl.OwnershipStatistics;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Test the capacity factors with the full stack.
 *
 * @author Dan Berindei
 * @since 6.0
 */
@Test(groups = "unstable", testName = "distribution.ch.CapacityFactorsFunctionalTest", description = "to be fixed by ISPN-6470")
public class CapacityFactorsFunctionalTest extends MultipleCacheManagersTest {

   public static final int NUM_SEGMENTS = 60;

   @Override
   protected void createCacheManagers() throws Throwable {
      // Do nothing here, create the cache managers in the test
   }

   public void testCapacityFactors() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.clustering().hash().numSegments(NUM_SEGMENTS);

      cb.clustering().hash().capacityFactor(0.5f);
      addClusterEnabledCacheManager(cb);
      waitForClusterToForm();
      assertCapacityFactors(0.5f);
      assertPrimaryOwned(NUM_SEGMENTS);
      assertOwned(NUM_SEGMENTS);

      cb.clustering().hash().capacityFactor(1.5f);
      addClusterEnabledCacheManager(cb);
      waitForClusterToForm();
      assertCapacityFactors(0.5f, 1.5f);
      assertPrimaryOwned(NUM_SEGMENTS / 4, NUM_SEGMENTS * 3 / 4);
      assertOwned(NUM_SEGMENTS, NUM_SEGMENTS);

      cb.clustering().hash().capacityFactor(0.0f);
      addClusterEnabledCacheManager(cb);
      waitForClusterToForm();
      assertCapacityFactors(0.5f, 1.5f, 0.0f);
      assertPrimaryOwned(NUM_SEGMENTS / 4, NUM_SEGMENTS * 3 / 4, 0);
      assertOwned(NUM_SEGMENTS, NUM_SEGMENTS, 0);

      cb.clustering().hash().capacityFactor(1.0f);
      addClusterEnabledCacheManager(cb);
      waitForClusterToForm();
      assertCapacityFactors(0.5f, 1.5f, 0.0f, 1.0f);
      assertPrimaryOwned(NUM_SEGMENTS / 6, NUM_SEGMENTS * 3 / 6, 0, NUM_SEGMENTS * 2 / 6);
      assertOwned(NUM_SEGMENTS / 3, NUM_SEGMENTS, 0, NUM_SEGMENTS * 2 / 3);
   }

   private void assertCapacityFactors(float... expectedCapacityFactors) {
      ConsistentHash ch = cache(0).getAdvancedCache().getDistributionManager().getCacheTopology().getReadConsistentHash();
      int numNodes = expectedCapacityFactors.length;
      Map<Address,Float> capacityFactors = ch.getCapacityFactors();
      for (int i = 0; i < numNodes; i++) {
         assertEquals(expectedCapacityFactors[i], capacityFactors.get(address(i)), 0.0);
      }
   }

   private void assertPrimaryOwned(int... expectedPrimaryOwned) {
      ConsistentHash ch = cache(0).getAdvancedCache().getDistributionManager().getCacheTopology().getReadConsistentHash();
      OwnershipStatistics stats = new OwnershipStatistics(ch, ch.getMembers());
      int numNodes = expectedPrimaryOwned.length;
      for (int i = 0; i < numNodes; i++) {
         double delta = expectedPrimaryOwned[i] * 0.15;
         assertBetween(expectedPrimaryOwned[i] - 2 * delta, expectedPrimaryOwned[i] + delta,
               stats.getPrimaryOwned(address(i)));
      }
   }

   private void assertOwned(int... expectedOwned) {
      ConsistentHash ch = cache(0).getAdvancedCache().getDistributionManager().getCacheTopology().getReadConsistentHash();
      OwnershipStatistics stats = new OwnershipStatistics(ch, ch.getMembers());
      int numNodes = expectedOwned.length;
      for (int i = 0; i < numNodes; i++) {
         double delta = expectedOwned[i] * 0.25;
         assertBetween(expectedOwned[i] - 2 * delta, expectedOwned[i] + delta, stats.getOwned(address(i)));
      }
   }
}
