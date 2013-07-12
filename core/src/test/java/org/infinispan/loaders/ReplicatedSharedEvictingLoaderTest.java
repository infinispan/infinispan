package org.infinispan.loaders;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * This test aims to ensure that a replicated cache with a shared loader, when using passivation and eviction, doesn't
 * remove entries from the cache store when activating.
 */
@Test(testName = "loaders.ReplicatedSharedEvictingLoaderTest", groups = "functional")
public class ReplicatedSharedEvictingLoaderTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC);
      c
         .loaders()
            .passivation(true)
            .shared(true)
            .addStore(DummyInMemoryCacheStoreConfigurationBuilder.class)
               .storeName("ReplicatedSharedEvictingLoaderTest");
      createCluster(c, 2);
      waitForClusterToForm();
   }

   public void testRemovalFromCacheStoreOnEvict() {
      cache(0).put("k", "v");

      assert "v".equals(cache(0).get("k"));
      assert "v".equals(cache(1).get("k"));

      cache(0).evict("k");
      cache(1).evict("k");

      assert "v".equals(cache(0).get("k"));
      assert "v".equals(cache(1).get("k"));
   }
}
