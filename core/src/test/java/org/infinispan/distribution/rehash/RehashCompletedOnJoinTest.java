package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Test(groups = "functional", testName = "distribution.rehash.RehashCompletedOnJoinTest")
public class RehashCompletedOnJoinTest extends BaseDistFunctionalTest<Object, String> {

   public RehashCompletedOnJoinTest() {
      INIT_CLUSTER_SIZE = 2;
      performRehashing = true;
   }

   public void testJoinComplete() {
      List<MagicKey> keys = new ArrayList<MagicKey>(Arrays.asList(
            new MagicKey("k1", c1), new MagicKey("k2", c2),
            new MagicKey("k3", c1), new MagicKey("k4", c2)
      ));

      int i = 0;
      for (Cache<Object, String> c : caches) c.put(keys.get(i++), "v" + i);
      log.infof("Initialized with keys %s", keys);
      
      EmbeddedCacheManager joinerManager = addClusterEnabledCacheManager();
      joinerManager.defineConfiguration(cacheName, configuration.build());
      Cache joiner = joinerManager.getCache(cacheName);
      DistributionManager dmi = joiner.getAdvancedCache().getDistributionManager();
      assert dmi.isJoinComplete();
   }

}
