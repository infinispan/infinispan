package org.infinispan.expiry;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.List;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Test(groups = "functional", testName = "expiry.ReplicatedExpiryTest")
public class ReplicatedExpiryTest extends MultipleCacheManagersTest {

   Cache c1, c2;

   protected void createCacheManagers() throws Throwable {
      Configuration cfg = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      List<Cache<Object, Object>> caches = createClusteredCaches(2, "cache", cfg);
      c1 = caches.get(0);
      c2 = caches.get(1);
   }

   public void testLifespanExpiryReplicates() throws InterruptedException {
      long lifespan = 3000;
      c1.put("k", "v", lifespan, MILLISECONDS);
      InternalCacheEntry ice = c2.getAdvancedCache().getDataContainer().get("k");

      assert ice instanceof MortalCacheEntry;
      assert ice.getLifespan() == lifespan;
      assert ice.getMaxIdle() == -1;
   }

   public void testIdleExpiryReplicates() throws InterruptedException {
      long idle = 3000;
      c1.put("k", "v", -1, MILLISECONDS, idle, MILLISECONDS);
      InternalCacheEntry ice = c2.getAdvancedCache().getDataContainer().get("k");

      assert ice instanceof TransientCacheEntry;
      assert ice.getMaxIdle() == idle;
      assert ice.getLifespan() == -1;
   }

   public void testBothExpiryReplicates() throws InterruptedException {
      long lifespan = 10000;
      long idle = 3000;
      c1.put("k", "v", lifespan, MILLISECONDS, idle, MILLISECONDS);
      InternalCacheEntry ice = c2.getAdvancedCache().getDataContainer().get("k");
      assert ice instanceof TransientMortalCacheEntry;
      assert ice.getLifespan() == lifespan;
      assert ice.getMaxIdle() == idle;
   }
}
