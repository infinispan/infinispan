package org.infinispan.loaders;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * This test aims to ensure that a replicated cache with a shared loader, when using passivation and eviction, doesn't
 * remove entries from the cache store when activating.
 */
@Test(testName = "loaders.ReplicatedSharedEvictingLoaderTest", groups = "functional")
public class ReplicatedSharedEvictingLoaderTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration c = new Configuration();
      c.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      clmc.addCacheLoaderConfig(new DummyInMemoryCacheStore.Cfg("ReplicatedSharedEvictingLoaderTest"));
      clmc.setShared(true);
      clmc.setPassivation(true);
      c.setCacheLoaderManagerConfig(clmc);
      createCluster(c, 2);
      TestingUtil.blockUntilViewsReceived(100000, cache(0), cache(1));
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
