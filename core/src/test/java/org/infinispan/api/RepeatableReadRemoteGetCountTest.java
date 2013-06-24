package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.CountingRpcManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "api.RepeatableReadRemoteGetCountTest")
public class RepeatableReadRemoteGetCountTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(false);
      builder.clustering().hash().numOwners(1);
      createClusteredCaches(2, builder);
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
      AssertJUnit.assertEquals("Wrong number of gets after put.", 0, rpcManager.clusterGet);

      AssertJUnit.assertEquals("Wrong value read.", "v1", cache.get(key));
      AssertJUnit.assertEquals("Wrong number of gets after read.", 0, rpcManager.clusterGet);

      AssertJUnit.assertEquals("Wrong put return value.", "v1", cache.put(key, "v2"));
      AssertJUnit.assertEquals("Wrong number of gets after put.", 0, rpcManager.clusterGet);

      AssertJUnit.assertEquals("Wrong replace return value.", "v2", cache.replace(key, "v3"));
      AssertJUnit.assertEquals("Wrong number of gets after replace.", 0, rpcManager.clusterGet);

      AssertJUnit.assertEquals("Wrong conditional replace return value.", true, cache.replace(key, "v3", "v4"));
      AssertJUnit.assertEquals("Wrong number of gets after conditional replace.", 0, rpcManager.clusterGet);

      AssertJUnit.assertEquals("Wrong conditional remove return value.", true, cache.remove(key, "v4"));
      AssertJUnit.assertEquals("Wrong number of gets after conditional remove.", 0, rpcManager.clusterGet);

      AssertJUnit.assertEquals("Wrong conditional put return value.", null, cache.putIfAbsent(key, "v5"));
      AssertJUnit.assertEquals("Wrong number of gets after conditional put.", 0, rpcManager.clusterGet);
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
      AssertJUnit.assertEquals("Wrong value read.", initialized ? "v1" : null, cache.get(key));
      AssertJUnit.assertEquals("Wrong number of gets after read.", 1, rpcManager.clusterGet);

      AssertJUnit.assertEquals("Wrong put return value.", initialized ? "v1" : null, cache.put(key, "v2"));
      AssertJUnit.assertEquals("Wrong number of gets after put.", 1, rpcManager.clusterGet);

      AssertJUnit.assertEquals("Wrong replace return value.", "v2", cache.replace(key, "v3"));
      AssertJUnit.assertEquals("Wrong number of gets after replace.", 1, rpcManager.clusterGet);

      AssertJUnit.assertEquals("Wrong conditional replace return value.", true, cache.replace(key, "v3", "v4"));
      AssertJUnit.assertEquals("Wrong number of gets after conditional replace.", 1, rpcManager.clusterGet);

      AssertJUnit.assertEquals("Wrong conditional remove return value.", true, cache.remove(key, "v4"));
      AssertJUnit.assertEquals("Wrong number of gets after conditional remove.", 1, rpcManager.clusterGet);

      AssertJUnit.assertEquals("Wrong conditional put return value.", null, cache.putIfAbsent(key, "v5"));
      AssertJUnit.assertEquals("Wrong number of gets after conditional put.", 1, rpcManager.clusterGet);
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
