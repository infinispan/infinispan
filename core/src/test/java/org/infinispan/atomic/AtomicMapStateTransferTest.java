package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Test modifications to an AtomicMap during state transfer are consistent.
 *
 * @author Dan Berindei
 * @since 7.0
 */
@Test(groups = "functional", testName = "atomic.AtomicMapStateTransferTest")
public class AtomicMapStateTransferTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();
      createClusteredCaches(1, "atomic", c);
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      return c;
   }

   public void testAtomicMapPutDuringJoin() throws ExecutionException, InterruptedException {
      Cache cache = cache(0, "atomic");
      ControlledRpcManager crm = new ControlledRpcManager(cache.getAdvancedCache().getRpcManager());
      TestingUtil.replaceComponent(cache, RpcManager.class, crm, true);

      MagicKey atomicMapKey = new MagicKey("atomicMapKey", cache);
      AtomicMap atomicMap = AtomicMapLookup.getAtomicMap(cache, atomicMapKey);
      atomicMap.put("key1", "value1");

      crm.blockBefore(StateResponseCommand.class);

      ConfigurationBuilder c = getConfigurationBuilder();
      final EmbeddedCacheManager joiner = addClusterEnabledCacheManager(c);
      Future<Cache> future = fork(new Callable<Cache>() {
         @Override
         public Cache call() throws Exception {
            return joiner.getCache("atomic");
         }
      });

      crm.waitForCommandToBlock();

      // Now we know state transfer will try to create an AtomicMap(key1=value1) on cache2
      // Insert another key in the atomic map, and check that cache2 has both keys after the state transfer
      atomicMap.put("key2", "value2");

      crm.stopBlocking();
      Cache cache2 = future.get();

      AtomicMap atomicMap2 = AtomicMapLookup.getAtomicMap(cache2, atomicMapKey);
      assertEquals(new HashSet<String>(Arrays.asList("key1", "key2")), atomicMap2.keySet());
   }
}
