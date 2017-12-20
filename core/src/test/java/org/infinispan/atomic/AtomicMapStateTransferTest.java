package org.infinispan.atomic;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ControlledRpcManager;
import org.testng.annotations.Test;

/**
 * Test modifications to an AtomicMap during state transfer are consistent.
 *
 * @author Dan Berindei
 * @since 7.0
 */
@Test(groups = "functional", testName = "atomic.AtomicMapStateTransferTest")
public class AtomicMapStateTransferTest extends MultipleCacheManagersTest {

   public static final String CACHE_NAME = "atomic";

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();
      createClusteredCaches(1, CACHE_NAME, c);
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      return c;
   }

   public void testAtomicMapPutDuringJoin() throws ExecutionException, InterruptedException {
      Cache cache = cache(0, CACHE_NAME);

      MagicKey atomicMapKey = new MagicKey("atomicMapKey", cache);
      AtomicMap atomicMap = AtomicMapLookup.getAtomicMap(cache, atomicMapKey);
      atomicMap.put("key1", "value1");

      ControlledRpcManager crm = ControlledRpcManager.replaceRpcManager(cache);
      crm.excludeCommands(StateRequestCommand.class, PrepareCommand.class, CommitCommand.class,
                          TxCompletionNotificationCommand.class);

      ConfigurationBuilder c = getConfigurationBuilder();
      final EmbeddedCacheManager joiner = addClusterEnabledCacheManager(c);
      joiner.defineConfiguration(CACHE_NAME, c.build());
      Future<Cache> future = fork(() -> joiner.getCache(CACHE_NAME));

      ControlledRpcManager.BlockedRequest blockedStateResponse = crm.expectCommand(StateResponseCommand.class);

      // Now we know state transfer will try to create an AtomicMap(key1=value1) on cache2
      // Insert another key in the atomic map, and check that cache2 has both keys after the state transfer
      atomicMap.put("key2", "value2");

      blockedStateResponse.send().receiveAll();
      Cache cache2 = future.get();

      AtomicMap atomicMap2 = AtomicMapLookup.getAtomicMap(cache2, atomicMapKey);
      assertEquals(new HashSet<>(Arrays.asList("key1", "key2")), atomicMap2.keySet());

      crm.stopBlocking();
   }
}
