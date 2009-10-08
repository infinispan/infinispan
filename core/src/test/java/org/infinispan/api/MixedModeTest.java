package org.infinispan.api;

import org.infinispan.AdvancedCache;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.MixedModeTest")
public class MixedModeTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      Configuration replSync = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      Configuration replAsync = getDefaultClusteredConfig(Configuration.CacheMode.REPL_ASYNC);
      Configuration invalSync = getDefaultClusteredConfig(Configuration.CacheMode.INVALIDATION_SYNC);
      Configuration invalAsync = getDefaultClusteredConfig(Configuration.CacheMode.INVALIDATION_ASYNC);
      Configuration local = getDefaultClusteredConfig(Configuration.CacheMode.LOCAL);

      createClusteredCaches(2, "replSync", replSync);
      defineConfigurationOnAllManagers("replAsync", replAsync);
      defineConfigurationOnAllManagers("invalSync", invalSync);
      defineConfigurationOnAllManagers("invalAsync", invalAsync);
      defineConfigurationOnAllManagers("local", local);

   }

   public void testMixedMode() {

      AdvancedCache replSyncCache1, replSyncCache2;
      AdvancedCache replAsyncCache1, replAsyncCache2;
      AdvancedCache invalAsyncCache1, invalAsyncCache2;
      AdvancedCache invalSyncCache1, invalSyncCache2;
      AdvancedCache localCache1, localCache2;

      replSyncCache1 = cache(0, "replSync").getAdvancedCache();
      replSyncCache2 = cache(1, "replSync").getAdvancedCache();
      replAsyncCache1 = cache(0, "replAsync").getAdvancedCache();
      replAsyncCache2 = cache(1, "replAsync").getAdvancedCache();
      invalSyncCache1 = cache(0, "invalSync").getAdvancedCache();
      invalSyncCache2 = cache(1, "invalSync").getAdvancedCache();
      invalAsyncCache1 = cache(0, "invalAsync").getAdvancedCache();
      invalAsyncCache2 = cache(1, "invalAsync").getAdvancedCache();
      localCache1 = cache(0, "local").getAdvancedCache();
      localCache2 = cache(1, "local").getAdvancedCache();

      invalSyncCache2.put("k", "v", Flag.CACHE_MODE_LOCAL);
      assert invalSyncCache2.get("k").equals("v");
      assert invalSyncCache1.get("k") == null;
      invalAsyncCache2.put("k", "v", Flag.CACHE_MODE_LOCAL);
      assert invalAsyncCache2.get("k").equals("v");
      assert invalAsyncCache1.get("k") == null;

      replListener(replAsyncCache2).expectAny();
      replListener(invalAsyncCache2).expectAny();

      replSyncCache1.put("k", "replSync");
      replAsyncCache1.put("k", "replAsync");
      invalSyncCache1.put("k", "invalSync");
      invalAsyncCache1.put("k", "invalAsync");
      localCache1.put("k", "local");

      replListener(replAsyncCache2).waitForRpc();
      replListener(invalAsyncCache2).waitForRpc();

      assert replSyncCache1.get("k").equals("replSync");
      assert replSyncCache2.get("k").equals("replSync");
      assert replAsyncCache1.get("k").equals("replAsync");
      assert replAsyncCache2.get("k").equals("replAsync");
      assert invalSyncCache1.get("k").equals("invalSync");
      assert invalSyncCache2.get("k") == null;
      assert invalAsyncCache1.get("k").equals("invalAsync");
      assert invalAsyncCache2.get("k") == null;
      assert localCache1.get("k").equals("local");
      assert localCache2.get("k") == null;
   }
}
