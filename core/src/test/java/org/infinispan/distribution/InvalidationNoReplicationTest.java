package org.infinispan.distribution;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.testng.Assert.*;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.InvalidationNoReplicationTest")
public class InvalidationNoReplicationTest extends MultipleCacheManagersTest {

   protected Object k0;
   protected boolean transactional = true;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, transactional);
      config.clustering().l1().enable().hash().numOwners(1);
      createCluster(config, 2);
      waitForClusterToForm();
      k0 = getKeyForCache(0);
   }

   public void testInvalidation() throws Exception {

      assertEquals(Collections.singletonList(address(0)), advancedCache(0).getDistributionManager().locate(k0, LookupMode.WRITE));
      assertEquals(Collections.singletonList(address(0)), advancedCache(1).getDistributionManager().locate(k0, LookupMode.WRITE));

      advancedCache(1).put(k0, "k1");
      assertTrue(advancedCache(1).getDataContainer().containsKey(k0));
      assertTrue(advancedCache(0).getDataContainer().containsKey(k0));

      tm(0).begin();
      cache(0).put(k0, "v2");
      tm(0).commit();

      assertFalse(advancedCache(1).getDataContainer().containsKey(k0));
   }

}
