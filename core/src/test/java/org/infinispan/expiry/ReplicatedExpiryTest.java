package org.infinispan.expiry;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheEntry;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiry.ReplicatedExpiryTest")
public class ReplicatedExpiryTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      createClusteredCaches(2, "cache", builder);
   }

   public void testLifespanExpiryReplicates() {
      Cache c1 = cache(0,"cache");
      Cache c2 = cache(1,"cache");
      long lifespan = 3000;
      c1.put("k", "v", lifespan, MILLISECONDS);
      InternalCacheEntry ice = c2.getAdvancedCache().getDataContainer().get("k");

      assert ice instanceof MortalCacheEntry || ice instanceof MetadataMortalCacheEntry;
      assert ice.getValue() != null;
      assert ice.getLifespan() == lifespan;
      assert ice.getMaxIdle() == -1;
   }

   public void testIdleExpiryReplicates() {
      Cache c1 = cache(0,"cache");
      Cache c2 = cache(1,"cache");
      long idle = 3000;
      c1.put("k", "v", -1, MILLISECONDS, idle, MILLISECONDS);
      InternalCacheEntry ice = c2.getAdvancedCache().getDataContainer().get("k");

      assert ice instanceof TransientCacheEntry || ice instanceof MetadataTransientCacheEntry;
      assert ice.getValue() != null;
      assert ice.getMaxIdle() == idle;
      assert ice.getLifespan() == -1;
   }

   public void testBothExpiryReplicates() {
      Cache c1 = cache(0,"cache");
      Cache c2 = cache(1,"cache");
      long lifespan = 10000;
      long idle = 3000;
      c1.put("k", "v", lifespan, MILLISECONDS, idle, MILLISECONDS);
      InternalCacheEntry ice = c2.getAdvancedCache().getDataContainer().get("k");
      assert ice instanceof TransientMortalCacheEntry || ice instanceof MetadataTransientMortalCacheEntry;
      assert ice.getValue() != null;
      assert ice.getLifespan() == lifespan : "Expected " + lifespan + " but was " + ice.getLifespan();
      assert ice.getMaxIdle() == idle : "Expected " + idle + " but was " + ice.getMaxIdle();
   }
}
