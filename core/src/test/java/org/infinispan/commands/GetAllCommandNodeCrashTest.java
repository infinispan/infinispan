package org.infinispan.commands;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commands.statetransfer.StateTransferCancelCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.ControlledRpcManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "commands.GetAllCommandNodeCrashTest")
public class GetAllCommandNodeCrashTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, TestDataSCI.INSTANCE, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
   }

   public void test() throws Exception {
      MagicKey key = new MagicKey(cache(0), cache(1));
      cache(2).put(key, "value");

      CheckPoint checkPoint = new CheckPoint();
      ControlledRpcManager rpcManager = ControlledRpcManager.replaceRpcManager(cache(2));
      rpcManager.excludeCommands(StateResponseCommand.class, StateTransferStartCommand.class,
                                 StateTransferCancelCommand.class);

      StateConsumer stateConsumerSpy = spy(extractComponent(cache(2), StateConsumer.class));
      doAnswer(invocation -> {
         checkPoint.trigger("topology_update_blocked");
         checkPoint.awaitStrict("topology_update_resumed", 10, TimeUnit.SECONDS);
         return invocation.callRealMethod();
      }).when(stateConsumerSpy).onTopologyUpdate(any(), anyBoolean());
      replaceComponent(cache(2), StateConsumer.class, stateConsumerSpy, true);

      Future<Map<Object, Object>> f = fork(() -> cache(2).getAdvancedCache().getAll(Collections.singleton(key)));

      // Block the request before being sent
      ControlledRpcManager.BlockedRequest<?> blockedGetAll = rpcManager.expectCommand(ClusteredGetAllCommand.class);

      // it's necessary to stop whole cache manager, not just cache, because otherwise the exception would have
      // suspect node defined
      cacheManagers.get(0).stop();
      checkPoint.awaitStrict("topology_update_blocked", 10, TimeUnit.SECONDS);

      // Send the blocked request and wait for the CacheNotFoundResponse
      blockedGetAll.send().receiveAll();

      // The retry can't be sent at this point
      rpcManager.expectNoCommand();

      // Resume the topology update
      checkPoint.trigger("topology_update_resumed");

      // Now the command can be retried, and the operation can finish
      rpcManager.expectCommand(ClusteredGetAllCommand.class).send().receiveAll();

      try {
         Map<Object, Object> map = f.get(10, TimeUnit.SECONDS);
         assertNotNull(map);
         assertFalse(map.isEmpty());
         assertEquals("value", map.get(key));
      } finally {
         checkPoint.triggerForever("topology_update_resumed");
         rpcManager.stopBlocking();
      }
   }
}
