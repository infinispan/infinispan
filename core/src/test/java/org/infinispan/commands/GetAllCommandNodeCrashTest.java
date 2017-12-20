package org.infinispan.commands;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.ControlledRpcManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "commands.GetAllCommandNodeCrashTest")
public class GetAllCommandNodeCrashTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
   }

   public void test() throws Exception {
      MagicKey key = new MagicKey(cache(0), cache(1));
      cache(2).put(key, "value");

      ControlledRpcManager rpcManager = ControlledRpcManager.replaceRpcManager(cache(2));

      CountDownLatch blockTopologyUpdate = new CountDownLatch(1);
      StateConsumer stateConsumerMock = spy(extractComponent(cache(2), StateConsumer.class));
      doAnswer(invocation -> {
         assertTrue(blockTopologyUpdate.await(10, TimeUnit.SECONDS));
         return invocation.callRealMethod();
      }).when(stateConsumerMock).onTopologyUpdate(any(), anyBoolean());
      replaceComponent(cache(2), StateConsumer.class, stateConsumerMock, true);

      StateTransferLock stateTransferLockMock = spy(extractComponent(cache(2), StateTransferLock.class));
      doAnswer(invocation -> {
         blockTopologyUpdate.countDown();
         return invocation.callRealMethod();
      }).when(stateTransferLockMock).topologyFuture(anyInt());
      replaceComponent(cache(2), StateTransferLock.class, stateTransferLockMock, true);

      Future<Map<Object, Object>> f = fork(() -> cache(2).getAdvancedCache().getAll(Collections.singleton(key)));

      ControlledRpcManager.BlockedRequest blockedGetAll = rpcManager.expectCommand(ClusteredGetAllCommand.class);
      // it's necessary to stop whole cache manager, not just cache, because otherwise the exception would have
      // suspect node defined
      cacheManagers.get(0).stop();

      // Send the blocked GetAllCommand and the retried one
      blockedGetAll.send().receiveAll();
      rpcManager.expectCommand(ClusteredGetAllCommand.class).send().receiveAll();

      try {
         Map<Object, Object> map = f.get(10, TimeUnit.SECONDS);
         assertNotNull(map);
         assertFalse(map.isEmpty());
         assertEquals("value", map.get(key));
      } finally {
         blockTopologyUpdate.countDown();
         rpcManager.stopBlocking();
      }
   }
}
