package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "distribution.rehash.WorkDuringJoinTest")
public class RehashCompletedOnJoinTest extends BaseDistFunctionalTest {

   public RehashCompletedOnJoinTest() {
      INIT_CLUSTER_SIZE = 2;
      performRehashing = true;
   }

   public void testJoinComplete() {
      List<MagicKey> keys = new ArrayList<MagicKey>(Arrays.asList(
            new MagicKey(c1, "k1"), new MagicKey(c2, "k2"),
            new MagicKey(c1, "k3"), new MagicKey(c2, "k4")
      ));

      int i = 0;
      for (Cache<Object, String> c : caches) c.put(keys.get(i++), "v" + i);
      log.info("Initialized with keys {0}", keys);
      
      EmbeddedCacheManager joinerManager = addClusterEnabledCacheManager();
      joinerManager.defineConfiguration(cacheName, configuration);
      Cache joiner = joinerManager.getCache(cacheName);
      DistributionManager dmi = joiner.getAdvancedCache().getDistributionManager();
      assert dmi.isJoinComplete() == true;
   }

}
