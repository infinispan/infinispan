package org.infinispan.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.CountingRpcManager;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "api.RepeatableReadRemoteGetCountTest")
public class RepeatableReadRemoteGetCountTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      builder.clustering().hash().numOwners(1);
      createClusteredCaches(2, TestDataSCI.INSTANCE, builder);
   }

   public void testOnKeyInitialized() throws Exception {
      doTest(true);
   }

   public void testOnKeyNonInitialized() throws Exception {
      doTest(false);
   }

   public void testWithoutReading() throws Exception {
      final Object key = new MagicKey("key", cache(0));
      final Cache<Object, Object> cache = cache(1);
      final TransactionManager tm = tm(1);
      final CountingRpcManager rpcManager = replaceRpcManager(cache);

      cache(0).put(key, "v0");

      tm.begin();
      rpcManager.resetStats();
      cache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES).put(key, "v1");
      assertEquals(0, rpcManager.clusterGet, "Wrong number of gets after put.");
      assertEquals("v1", cache.get(key), "Wrong value read.");
      assertEquals(0, rpcManager.clusterGet, "Wrong number of gets after read.");

      assertEquals("v1", cache.put(key, "v2"), "Wrong put return value.");
      assertEquals(0, rpcManager.clusterGet, "Wrong number of gets after put.");

      assertEquals("v2", cache.replace(key, "v3"), "Wrong replace return value.");
      assertEquals(0, rpcManager.clusterGet, "Wrong number of gets after replace.");

      assertTrue(cache.replace(key, "v3", "v4"), "Wrong conditional replace return value.");
      assertEquals(0, rpcManager.clusterGet, "Wrong number of gets after conditional replace.");

      assertTrue(cache.remove(key, "v4"), "Wrong conditional remove return value.");
      assertEquals(0, rpcManager.clusterGet, "Wrong number of gets after conditional remove.");

      assertNull(cache.putIfAbsent(key, "v5"), "Wrong conditional put return value.");
      assertEquals(0, rpcManager.clusterGet, "Wrong number of gets after conditional put.");
      tm.commit();
   }

   private void doTest(boolean initialized) throws Exception {
      final Object key = new MagicKey("key", cache(0));
      final Cache<Object, Object> cache = cache(1);
      final TransactionManager tm = tm(1);
      final CountingRpcManager rpcManager = replaceRpcManager(cache);

      if (initialized) {
         cache(0).put(key, "v1");
      }

      tm.begin();
      rpcManager.resetStats();
      assertEquals(initialized ? "v1" : null, cache.get(key), "Wrong value read.");
      assertEquals(1, rpcManager.clusterGet, "Wrong number of gets after read.");

      assertEquals(initialized ? "v1" : null, cache.put(key, "v2"), "Wrong put return value.");
      assertEquals(1, rpcManager.clusterGet, "Wrong number of gets after put.");

      assertEquals("v2", cache.replace(key, "v3"), "Wrong replace return value.");
      assertEquals(1, rpcManager.clusterGet, "Wrong number of gets after replace.");

      assertTrue(cache.replace(key, "v3", "v4"), "Wrong conditional replace return value.");
      assertEquals(1, rpcManager.clusterGet, "Wrong number of gets after conditional replace.");

      assertTrue(cache.remove(key, "v4"), "Wrong conditional remove return value.");
      assertEquals(1, rpcManager.clusterGet, "Wrong number of gets after conditional remove.");

      assertNull(cache.putIfAbsent(key, "v5"), "Wrong conditional put return value.");
      assertEquals(1, rpcManager.clusterGet, "Wrong number of gets after conditional put.");
      tm.commit();
   }

   private CountingRpcManager replaceRpcManager(Cache cache) {
      RpcManager current = TestingUtil.extractComponent(cache, RpcManager.class);
      if (current instanceof CountingRpcManager) {
         return (CountingRpcManager) current;
      }
      CountingRpcManager countingRpcManager = new CountingRpcManager(current);
      TestingUtil.replaceComponent(cache, RpcManager.class, countingRpcManager, true);
      return countingRpcManager;
   }
}
