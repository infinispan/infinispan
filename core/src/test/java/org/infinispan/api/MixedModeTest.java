package org.infinispan.api;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.MixedModeTest")
public class MixedModeTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder replSync = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      ConfigurationBuilder replAsync = getDefaultClusteredCacheConfig(CacheMode.REPL_ASYNC, false);
      ConfigurationBuilder invalSync = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, false);
      ConfigurationBuilder invalAsync = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_ASYNC, false);
      ConfigurationBuilder local = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);

      createClusteredCaches(2, "replSync", replSync);
      defineConfigurationOnAllManagers("replAsync", replAsync);
      waitForClusterToForm("replAsync");
      defineConfigurationOnAllManagers("invalSync", invalSync);
      waitForClusterToForm("invalSync");
      defineConfigurationOnAllManagers("invalAsync", invalAsync);
      waitForClusterToForm("invalAsync");
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

      // With the default SyncConsistentHashFactory, the same key will work for all caches
      MagicKey key = new MagicKey("k", replAsyncCache1);
      invalSyncCache2.withFlags(CACHE_MODE_LOCAL).put(key, "v");
      assertEquals("v", invalSyncCache2.get(key));
      assertNull(invalSyncCache1.get(key));
      invalAsyncCache2.withFlags(CACHE_MODE_LOCAL).put(key, "v");
      assertEquals("v", invalAsyncCache2.get(key));
      assertNull(invalAsyncCache1.get(key));

      replListener(replAsyncCache2).expectAny();
      replListener(invalAsyncCache2).expectAny();

      replSyncCache1.put(key, "replSync");
      replAsyncCache1.put(key, "replAsync");
      invalSyncCache1.put(key, "invalSync");
      invalAsyncCache1.put(key, "invalAsync");
      localCache1.put(key, "local");

      replListener(replAsyncCache2).waitForRpc();
      replListener(invalAsyncCache2).waitForRpc();

      assertEquals("replSync", replSyncCache1.get(key));
      assertEquals("replSync", replSyncCache2.get(key));
      assertEquals("replAsync", replAsyncCache1.get(key));
      assertEquals("replAsync", replAsyncCache2.get(key));
      assertEquals("invalSync", invalSyncCache1.get(key));
      assertNull(invalSyncCache2.get(key));
      assertEquals("invalAsync", invalAsyncCache1.get(key));
      assertNull(invalAsyncCache2.get(key));
      assertEquals("local", localCache1.get(key));
      assertNull(localCache2.get(key));
   }
}
