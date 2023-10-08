package org.infinispan.commands;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.ControlledRpcManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "commands.GetAllCacheNotFoundResponseTest")
public class GetAllCacheNotFoundResponseTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      ControlledConsistentHashFactory.Default chf = new ControlledConsistentHashFactory.Default(
            new int[][]{{0, 1}, {0, 2}, {2, 3}});
      cb.clustering().hash().numOwners(2).numSegments(3).consistentHashFactory(chf);
      createClusteredCaches(5, ControlledConsistentHashFactory.SCI.INSTANCE, cb);
   }

   public void test() throws InterruptedException, ExecutionException, TimeoutException {
      ControlledRpcManager crm4 = ControlledRpcManager.replaceRpcManager(cache(4));
      crm4.excludeCommands(StateResponseCommand.class);

      MagicKey key1 = new MagicKey(cache(0), cache(1));
      MagicKey key2 = new MagicKey(cache(0), cache(2));
      MagicKey key3 = new MagicKey(cache(2), cache(3));

      // We expect the targets of ClusteredGetAllCommands to be selected in a specific way and for that we need
      // to iterate through the keys in certain order.
      Set<Object> keys = new LinkedHashSet<>(Arrays.asList(key1, key2, key3));
      Future<Map<Object, Object>> future = fork(() -> cache(4).getAdvancedCache().getAll(keys));

      // Wait until the first two ClusteredGetAllCommands are sent
      log.debugf("Expect first get all");
      ControlledRpcManager.BlockedRequests round1 =
         crm4.expectCommands(ClusteredGetAllCommand.class, address(0), address(2));
      // Provide fake responses for the 1st round
      round1.skipSendAndReceive(address(0), CacheNotFoundResponse.INSTANCE);
      round1.skipSendAndReceiveAsync(address(2), UnsureResponse.INSTANCE);

      // Key retries are independent: will retry key1 on cache1, key2 on cache2, and key3 on cache3
      log.debugf("Expect 1st retry");
      ControlledRpcManager.BlockedRequests round2 =
         crm4.expectCommands(ClusteredGetAllCommand.class, address(1), address(2), address(3));
      // Provide a response for the retry commands.
      // We simulate that key1 is completely lost due to crashing nodes.
      round2.skipSendAndReceive(address(1), CacheNotFoundResponse.INSTANCE);
      round2.skipSendAndReceive(address(2), SuccessfulResponse.create(new InternalCacheValue[]{new ImmortalCacheValue("value2")}));
      round2.skipSendAndReceiveAsync(address(3), SuccessfulResponse.create(new InternalCacheValue[]{new ImmortalCacheValue("value3")}));

      // After all the owners are lost, we must wait for a new topology in case the key is still available
      crm4.expectNoCommand(10, TimeUnit.MILLISECONDS);

      log.debugf("Increment topology and expect 2nd retry");
      Future<Void> topologyUpdateFuture = simulateTopologyUpdate(cache(4));

      ControlledRpcManager.BlockedRequests round3 =
         crm4.expectCommands(ClusteredGetAllCommand.class, address(0));
      // Provide a response for the 2nd retry
      // Because we only simulated the loss of cache0, the primary owner is the same
      round3.skipSendAndReceive(address(0), SuccessfulResponse.create(new InternalCacheValue[]{null}));

      log.debugf("Expect final result");
      topologyUpdateFuture.get(10, TimeUnit.SECONDS);
      Map<Object, Object> values = future.get(10, TimeUnit.SECONDS);
      // assertEquals is more verbose than assertNull in case of failure
      assertEquals(null, values.get(key1));
      assertEquals("value2", values.get(key2));
      assertEquals("value3", values.get(key3));
   }

   private Future<Void> simulateTopologyUpdate(Cache<Object, Object> cache) {
      StateTransferLock stl4 = TestingUtil.extractComponent(cache, StateTransferLock.class);
      DistributionManager dm4 = cache.getAdvancedCache().getDistributionManager();
      LocalizedCacheTopology cacheTopology = dm4.getCacheTopology();
      int newTopologyId = cacheTopology.getTopologyId() + 1;
      CacheTopology newTopology = new CacheTopology(newTopologyId, cacheTopology.getRebalanceId(),
                                                    cacheTopology.getCurrentCH(), cacheTopology.getPendingCH(),
                                                    cacheTopology.getUnionCH(),
                                                    cacheTopology.getPhase(), cacheTopology.getActualMembers(),
                                                    cacheTopology.getMembersPersistentUUIDs());
      dm4.setCacheTopology(newTopology);
      return fork(() -> stl4.notifyTransactionDataReceived(newTopologyId));
   }
}
