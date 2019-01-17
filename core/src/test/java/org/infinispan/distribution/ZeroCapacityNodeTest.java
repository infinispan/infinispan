package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Map;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.group.impl.PartitionerConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Test the capacity factor for lite instance
 *
 * @author Katia Aresti
 * @since 9.4
 */
@Test(groups = "functional", testName = "distribution.ch.ZeroCapacityNodeTest")
public class ZeroCapacityNodeTest extends MultipleCacheManagersTest {

   public static final int NUM_SEGMENTS = 60;

   @Override
   protected void createCacheManagers() throws Throwable {
      // Do nothing here, create the cache managers in the test
   }

   public void testCapacityFactorContainingAZeroCapacityNode() {

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.clustering().hash().numSegments(NUM_SEGMENTS);
      cb.clustering().hash().capacityFactor(0.5f);

      EmbeddedCacheManager node1 = addClusterEnabledCacheManager(GlobalConfigurationBuilder.defaultClusteredBuilder(), cb);
      EmbeddedCacheManager node2 = addClusterEnabledCacheManager(GlobalConfigurationBuilder.defaultClusteredBuilder(), cb);
      EmbeddedCacheManager zeroCapacityNode = addClusterEnabledCacheManager(GlobalConfigurationBuilder.defaultClusteredBuilder().zeroCapacityNode(true), cb);

      waitForClusterToForm();
      assertCapacityFactors(node1, 0.5f);
      assertCapacityFactors(node2, 0.5f);
      assertCapacityFactors(zeroCapacityNode, 0.0f);
   }

   private void assertCapacityFactors(EmbeddedCacheManager cm, float expectedCapacityFactors) {
      ConsistentHash ch = cache(0).getAdvancedCache().getDistributionManager().getReadConsistentHash();
      DefaultConsistentHash dch =
            (DefaultConsistentHash) TestingUtil.extractField(PartitionerConsistentHash.class, ch, "ch");
      Map<Address, Float> capacityFactors = dch.getCapacityFactors();
      assertEquals(expectedCapacityFactors, capacityFactors.get(cm.getAddress()), 0.0);
   }
}
