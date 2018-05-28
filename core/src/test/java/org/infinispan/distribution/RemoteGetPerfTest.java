package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "profiling", testName = "distribution.RemoteGetPerfTest")
public class RemoteGetPerfTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false), 3);
      waitForClusterToForm();
   }

   public void testRepeatedRemoteGet() {
      String key = "key";
      List<Address> owners = cache(0).getAdvancedCache().getDistributionManager().locate(key);
      Cache<Object, Object> nonOwnerCache = caches().stream()
                                                    .filter(c -> !owners.contains(address(c)))
                                                    .findAny()
                                                    .orElse(null);
      cache(0).put(key, "value");
      for (int i = 0; i < 50000; i++) {
         assertEquals("value", nonOwnerCache.get(key));
      }
   }
}
