package org.infinispan.distribution;

import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.RndKeyGenerator;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.Executors;

import static org.testng.Assert.assertEquals;

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
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, transactional);
      config.setL1CacheEnabled(true);
      config.setNumOwners(1);
      createCluster(config, 2);
      waitForClusterToForm();
      k0 = getKeyForCache(0);
   }

   public void testInvalidation() throws Exception {

      assert advancedCache(0).getDistributionManager().locate(k0).equals(Collections.singletonList(address(0)));
      assert advancedCache(1).getDistributionManager().locate(k0).equals(Collections.singletonList(address(0)));

      advancedCache(1).put(k0, "k1");
      assert advancedCache(1).getDataContainer().containsKey(k0);
      assert advancedCache(0).getDataContainer().containsKey(k0);

      tm(0).begin();
      cache(0).put(k0, "v2");
      tm(0).commit();

      assert !advancedCache(1).getDataContainer().containsKey(k0);
   }

}
