package org.horizon.expiry;

import org.horizon.Cache;
import org.horizon.config.Configuration;
import org.horizon.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Test(groups = "functional", testName = "expiry.ReplicatedExpiryTest")
public class ReplicatedExpiryTest extends MultipleCacheManagersTest {

   Cache c1, c2;

   protected void createCacheManagers() throws Throwable {
      Configuration cfg = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      List<Cache> caches = createClusteredCaches(2, "cache", cfg);
      c1 = caches.get(0);
      c2 = caches.get(1);
   }

   public void testExpiryReplicates() throws InterruptedException {
      long start = System.currentTimeMillis();
      long lifespan = 3000;
      c1.put("k", "v", lifespan, TimeUnit.MILLISECONDS);

      while (System.currentTimeMillis() < start + lifespan) {
         assert c1.get("k").equals("v");
         assert c2.get("k").equals("v");
         Thread.sleep(250);
      }
      Thread.sleep(1000);
      assert c1.get("k") == null;
      assert c2.get("k") == null;
   }
}
