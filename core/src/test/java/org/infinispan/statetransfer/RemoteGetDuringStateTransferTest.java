package org.infinispan.statetransfer;

import static org.infinispan.distribution.DistributionTestHelper.isFirstOwner;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.infinispan.test.fwk.TestCacheManagerFactory.DEFAULT_CACHE_NAME;
import static org.infinispan.util.BlockingLocalTopologyManager.confirmTopologyUpdate;
import static org.infinispan.util.BlockingLocalTopologyManager.finishRebalance;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.infinispan.Cache;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateCacheConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.globalstate.NoOpGlobalConfigurationManager;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.topology.CacheTopology.Phase;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Test multiple possible situations of interleaving between a remote get and state transfer.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "statetransfer.RemoteGetDuringStateTransferTest")
@CleanupAfterMethod
public class RemoteGetDuringStateTransferTest extends MultipleCacheManagersTest {
   private final List<BlockingLocalTopologyManager> topologyManagerList =
         Collections.synchronizedList(new ArrayList<>(4));
   private final List<ControlledRpcManager> rpcManagerList =
         Collections.synchronizedList(new ArrayList<>(4));

   /*
   Summary

   T0   initial topology (NO_REBALANCE, ro = wo = [1])
   T1   state transfer started (READ_OLD_WRITE_ALL, ro = [1], wo = [1, 2])
   T2   state transfer finished but rebalance not complete (READ_ALL_WRITE_ALL, ro = wo = [1, 2])
   T3   read new, write all topology (READ_NEW_WRITE_ALL, ro = [2], wo = [1, 2])
   T4   rebalance completed (NO_REBALANCE, ro = wo = [2])

   | sc     | first request | process request 1 | receive response 1 | retry | process request 2 | receive response 2 |
   | 010    | T0            | 1:T1              | T0                 | N1    |                   |                    |
   | 011    | T0            | 1:T1              | T1/T2/T3/T4        | N1    |                   |                    |
   | [2]    | T0            | 1:T2              | T0                 |       |                   |                    |
   | [1]    | T0            | 1:T2              | T1/T2/T3/T4        | N1    |                   |                    |
   | [2]    | T0            | 1:T3              | T0/T1              |       |                   |                    |
   | [2]    | T0            | 1:T3              | T2                 | Y*    | 2:T0/T1           |                    |
   | [4]    | T0            | 1:T3              | T2                 | Y*    | 2:T2              | T0/T1              |
   | 032_22 | T0            | 1:T3              | T2                 | Y*    | 2:T2              | T2/T3/T4           |
   | 032_32 | T0            | 1:T3              | T2                 | Y*    | 2:T3/T4           | T2/T3/T4           |
   | [2]    | T0            | 1:T3              | T3                 | Y     | 2:T0/T1           |                    |
   | [4]    | T0            | 1:T3              | T3                 | Y     | 2:T2              | T0/T1/T2           |
   | 033_23 | T0            | 1:T3              | T3                 | Y     | 2:T2              | T3/T4              |
   | [4]    | T0            | 1:T3              | T3                 | Y     | 2:T3/T4           | T0/T1/T2           |
   | 033_33 | T0            | 1:T3              | T3                 | Y     | 2:T3/T4           | T3/T4              |
   | [2]    | T0            | 1:T3              | T4                 | Y     | 2:T0/T1/T2        |                    |
   | [4]    | T0            | 1:T3              | T4                 | Y     | 2:T3/T4           | T0/T1/T2/T3        |
   | [1]    | T0            | 1:T3              | T4                 | Y     | 2:T3/T4           | T4                 |
   | [2]    | T0            | 1:T4              | T0/T1/T2           |       |                   |                    |
   | [4]    | T0            | 1:T4              | T3/T4              | Y     | 2:T0/T1/T2        |                    |
   | [2]    | T0            | 1:T4              | T3/T4              | Y     | 2:T3/T4           | T0/T1/T2           |
   | [1]    | T0            | 1:T4              | T3/T4              | Y     | 2:T3/T4           | T3/T4              |
   | [4]    | T1            | 1:T0              | T0                 |       |                   |                    |
   | 101    | T1            | 1:T0              | T1/T2/T3/T4        | N1    |                   |                    |
   | [4]    | T1            | 1:T1              | T0                 |       |                   |                    |
   | 111    | T1            | 1:T1              | T1/T2/T3/T4        |       |                   |                    |
   | [4]    | T1            | 1:T2              | T0                 |       |                   |                    |
   | [1]    | T1            | 1:T2              | T1/T2/T3/T4        | N1    |                   |                    |
   | [2]    | T1            | 1:T3              | T2                 | Y*    | 2:T0/T1           |                    |
   | [4]    | T1            | 1:T3              | T2                 | Y*    | 2:T2              | T0/T1              |
   | 132_22 | T1            | 1:T3              | T2                 | Y*    | 2:T2              | T2/T3/T4           |
   | [4]    | T1            | 1:T3              | T2                 | Y*    | 2:T3              | T0/T1              |
   | 132_32 | T1            | 1:T3              | T2                 | Y*    | 2:T3              | T2/T3/T4           |
   | [2]    | T1            | 1:T3              | T2                 | Y*    | 2:T4              | T0/T1/T2           |
   | [1]    | T1            | 1:T3              | T2                 | Y*    | 2:T4              | T3/T4              |
   | [2]    | T1            | 1:T3              | T3                 | Y     | 2:T0/T1           |                    |
   | [4]    | T1            | 1:T3              | T3                 | Y     | 2:T2              | T0/T1/T2           |
   | 133_23 | T1            | 1:T3              | T3                 | Y     | 2:T2              | T3/T4              |
   | [4]    | T1            | 1:T3              | T3                 | Y     | 2:T3              | T0/T1/T2           |
   | 133_33 | T1            | 1:T3              | T3                 | Y     | 2:T3              | T3/T4              |
   | [4]    | T1            | 1:T3              | T3                 | Y     | 2:T4              | T0/T1/T2           |
   | [1]    | T1            | 1:T3              | T3                 | Y     | 2:T4              | T3/T4              |
   | [2]    | T1            | 1:T3              | T4                 | Y     | 2:T0/T1/T2        |                    |
   | [1]    | T1            | 1:T3              | T4                 | Y     | 2:T3/T4           | T4                 |
   | [4]    | T1            | 1:T4              | T0/T1/T2           |       |                   |                    |
   | [2]    | T1            | 1:T4              | T3/T4              | Y     | 2:T0/T1/T2        |                    |
   | [4]    | T1            | 1:T4              | T3/T4              | Y     | 2:T3/T4           | T0/T1/T2           |
   | [1]    | T1            | 1:T4              | T3/T4              | Y     | 2:T3/T4           | T3/T4              |
   | [2]    | T2            | 1:T0              |                    |       |                   |                    |
   | [4]    | T2            | 1: *, 2:  *       | T0/T1              |       |                   |                    |
   | 2112   | T2            | 1:T1, 2: T1       | T2/T3/T4           | N1    |                   |                    |
   | 2122   | T2            | 1:T1, 2: T2       | T2/T3/T4           | N1T2  |                   |                    |
   | 2132   | T2            | 1:T1, 2: T3/T4    | T2/T3/T4           | N1T2  |                   |                    |
   | 2212   | T2            | 1:T2, 2: T1       | T2/T3/T4           | N1    |                   |                    |
   | 2222   | T2            | 1:T2, 2: T2       | T2/T3/T4           | N1T2  |                   |                    |
   | 2232   | T2            | 1:T2, 2: T3/T4    | T2/T3/T4           | N1T2  |                   |                    |
   | 2312_22| T2            | 1:T3/T4, 2: T1    | T2/T3/T4           | Y     | 2: T2             | T2/T3/T4           |
   | 2312_32| T2            | 1:T3/T4, 2: T1    | T2/T3/T4           | Y     | 2: T3/T4          | T2/T3/T4           |
   | 2322   | T2            | 1:T3/T4, 2: T2    | T2/T3/T4           | N2    |                   |                    |
   | 2332   | T2            | 1:T3/T4, 2: T3/T4 | T2/T3/T4           | N2    |                   |                    |
   | [2]    | T3            | 2: T0/T1          |                    |       |                   |                    |
   | [4]    | T3            | 2: T2             | T0/T1/T2           |       |                   |                    |
   | 323    | T3            | 2: T2             | T3/T4              | N2    |                   |                    |
   | [4]    | T3            | 2: T3/T4          | T0/T1/T2           |       |                   |                    |
   | 333    | T3            | 2: T3/T4          | T3/T4              | N2    |                   |                    |
   | [2]    | T4            | 2: T0/T1/T2       |                    |       |                   |                    |
   | [4]    | T4            | 2:T3              | T0/T1/T2/T3        | N2    |                   |                    |
   | 434    | T4            | 2:T3              | T4                 | N2    |                   |                    |

   *) The retry will go to both node 1 and 2 but 1 in T3 will respond with UnsureResponse
   [1] too similar to the previous scenario
   [2] impossible because we topologies can't differ by more than 1 at the same time
   [4] impossible, first response was received in later topology than second response
   N1/N2/N1T2 We won't do a retry because we got successful response from node 1/2/both 1 and 2

   A note for 2312_x2: while the two nodes cannot have topologies 3 and 1 at the same time, the two reads can arrive
   at different times there.
    */

   @AfterMethod(alwaysRun = true)
   public final void unblockAll() {
      //keep track of all controlled components. In case of failure, we need to unblock all otherwise we have to wait
      //long time until the test is able to stop all cache managers.
      for (BlockingLocalTopologyManager topologyManager : topologyManagerList) {
         topologyManager.stopBlocking();
      }
      topologyManagerList.clear();
      for (ControlledRpcManager rpcManager : rpcManagerList) {
         rpcManager.stopBlocking();
      }
      rpcManagerList.clear();
   }

   /**
    * ISPN-3315: In this scenario, a remote get is triggered and the reply received in a stable state. the old owner
    * receives the request after the rebalance_start command.
    */
   public void testScenario_010() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_010";
      ownerCheckAndInit(cache(1), key, "v");

      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final BlockingLocalTopologyManager topologyManager1 = replaceTopologyManager(manager(1));
      final int currentTopologyId = currentTopologyId(cache(0));

      extractInterceptorChain(cache(0)).addInterceptorAfter(new AssertNoRetryInterceptor(), StateTransferInterceptor.class);

      //remote get is sent in topology T0
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      ControlledRpcManager.BlockedRequest blockedGet = rpcManager0.expectCommand(ClusteredGetCommand.class);

      FailReadsInterceptor fri = new FailReadsInterceptor();
      NewNode joiner = addNode(g -> TestCacheManagerFactory.addInterceptor(g, DEFAULT_CACHE_NAME::equals, fri, TestCacheManagerFactory.InterceptorPosition.FIRST, null), null);

      // Install topology T1 on node 1 and unblock the remote get
      confirmTopologyUpdate(Phase.READ_OLD_WRITE_ALL, topologyManager1);
      awaitForTopology(currentTopologyId + 1, cache(1));
      blockedGet.send().receiveAll();

      //check the value returned and make sure that the requestor is still in currentTopologyId (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      fri.assertNotHit();
      assertTopologyId(currentTopologyId, cache(0));

      // Finish the rebalance
      confirmTopologyUpdate(Phase.READ_OLD_WRITE_ALL, topologyManager0, joiner.topologyManager);
      finishRebalance(Phase.READ_ALL_WRITE_ALL, topologyManager0, topologyManager1, joiner.topologyManager);

      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3315: similar to scenario 010, the remote get is triggered in stable state but reply is received after the
    * rebalance_start command. As in scenario 010, the owner receives the request after the rebalance_start command.
    */
   public void testScenario_011() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_011";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final BlockingLocalTopologyManager topologyManager1 = replaceTopologyManager(manager(1));
      final int currentTopologyId = currentTopologyId(cache(0));

      extractInterceptorChain(cache(0))
            .addInterceptorAfter(new AssertNoRetryInterceptor(), StateTransferInterceptor.class);

      //the remote get is triggered in the current topology id.
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      ControlledRpcManager.BlockedRequest blockedGet = rpcManager0.expectCommand(ClusteredGetCommand.class);

      FailReadsInterceptor fri = new FailReadsInterceptor();
      NewNode joiner = addNode(g -> TestCacheManagerFactory.addInterceptor(g, DEFAULT_CACHE_NAME::equals, fri, TestCacheManagerFactory.InterceptorPosition.FIRST, null), null);

      confirmTopologyUpdate(Phase.READ_OLD_WRITE_ALL, topologyManager0, topologyManager1);

      //wait until the rebalance start arrives in old owner and in the requestor. then let the remote get go.
      awaitForTopology(currentTopologyId + 1, cache(1));
      awaitForTopology(currentTopologyId + 1, cache(0));
      blockedGet.send().receiveAll();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      fri.assertNotHit();
      assertTopologyId(currentTopologyId + 1, cache(1));
      assertTopologyId(currentTopologyId + 1, cache(0));

      // Finish the rebalance
      joiner.topologyManager.confirmTopologyUpdate(Phase.READ_OLD_WRITE_ALL);
      finishRebalance(Phase.READ_ALL_WRITE_ALL, topologyManager0, topologyManager1, joiner.topologyManager);

      joiner.joinerFuture.get();
   }

   public void testScenario_101() throws Exception {
      testScenario_1x1(0);
   }

   public void testScenario_111() throws Exception {
      testScenario_1x1(1);
   }

   protected void testScenario_1x1(int topologyOnNode1) throws Exception {
      assertTrue(0 <= topologyOnNode1 && topologyOnNode1 <= 1);
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = String.format("key_1%d1", topologyOnNode1);
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final BlockingLocalTopologyManager topologyManager1 = replaceTopologyManager(manager(1));
      final int currentTopologyId = currentTopologyId(cache(0));

      extractInterceptorChain(cache(0))
              .addInterceptorAfter(new AssertNoRetryInterceptor(), StateTransferInterceptor.class);

      FailReadsInterceptor fri = new FailReadsInterceptor();
      NewNode joiner = addNode(g -> TestCacheManagerFactory.addInterceptor(g, DEFAULT_CACHE_NAME::equals, fri, TestCacheManagerFactory.InterceptorPosition.FIRST, null), null);

      // Install topology T1 on node 0 and maybe on node 1 as well
      topologyManager0.confirmTopologyUpdate(Phase.READ_OLD_WRITE_ALL);
      if (topologyOnNode1 > 0) {
         topologyManager1.confirmTopologyUpdate(Phase.READ_OLD_WRITE_ALL);
      }
      awaitForTopology(currentTopologyId + 1, cache(0));
      awaitForTopology(currentTopologyId + topologyOnNode1, cache(1));

      //the remote get is triggered after the rebalance_start and before the confirm_rebalance.
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      ControlledRpcManager.BlockedRequest blockedGet = rpcManager0.expectCommand(ClusteredGetCommand.class);

      blockedGet.send().receiveAll();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      fri.assertNotHit();
      assertTopologyId(currentTopologyId + 1, cache(0));

      if (topologyOnNode1 < 1) {
         topologyManager1.confirmTopologyUpdate(Phase.READ_OLD_WRITE_ALL);
      }
      joiner.topologyManager.confirmTopologyUpdate(Phase.READ_OLD_WRITE_ALL);
      finishRebalance(Phase.READ_ALL_WRITE_ALL, topologyManager0, topologyManager1, joiner.topologyManager);

      joiner.joinerFuture.get();
   }

   public void testScenario_032_22() throws Exception {
      testScenario_03x_yx(2, 2);
   }

   public void testScenario_032_32() throws Exception {
      testScenario_03x_yx(2, 3);
   }

   public void testScenario_033_23() throws Exception {
      testScenario_03x_yx(3, 2);
   }

   public void testScenario_033_33() throws Exception {
      testScenario_03x_yx(3, 3);
   }

   protected void testScenario_03x_yx(int topologyOnNode0, int topologyOnNode2) throws Exception {
      assertTrue(2 <= topologyOnNode0 && topologyOnNode0 <= 3);
      assertTrue(2 <= topologyOnNode2 && topologyOnNode2 <= 3);
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = String.format("key_03%d_%d%d", topologyOnNode0, topologyOnNode2, topologyOnNode0);
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final BlockingLocalTopologyManager topologyManager1 = replaceTopologyManager(manager(1));
      final int currentTopologyId = currentTopologyId(cache(0));

      //consistency check. the remote get is triggered
      assertTopologyId(currentTopologyId, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      ControlledRpcManager.BlockedRequest blockedGet = rpcManager0.expectCommand(ClusteredGetCommand.class);
      NewNode joiner = addNode(g -> TestCacheManagerFactory.addInterceptor(g, DEFAULT_CACHE_NAME::equals, new WaitForTopologyInterceptor(currentTopologyId + topologyOnNode2), TestCacheManagerFactory.InterceptorPosition.FIRST, null), null);

      confirmTopologyUpdate(Phase.READ_OLD_WRITE_ALL, topologyManager0, topologyManager1, joiner.topologyManager);
      confirmTopologyUpdate(Phase.READ_ALL_WRITE_ALL, topologyManager0, topologyManager1, joiner.topologyManager);

      if (topologyOnNode0 > 2) {
         topologyManager0.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }
      if (topologyOnNode2 > 2) {
         joiner.topologyManager.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }

      topologyManager1.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);

      // Wait for all the nodes to have the required topology
      awaitForTopology(currentTopologyId + 3, cache(1));
      awaitForTopology(currentTopologyId + topologyOnNode0, cache(0));
      blockedGet.send().receiveAll();

      // Allow the retry to proceed normally
      rpcManager0.expectCommand(ClusteredGetCommand.class).send().receiveAll();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + topologyOnNode0, cache(0));

      // Finish the rebalance
      if (topologyOnNode0 < 3) {
         topologyManager0.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }
      if (topologyOnNode2 < 3) {
         joiner.topologyManager.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }
      finishRebalance(Phase.NO_REBALANCE, topologyManager0, topologyManager1, joiner.topologyManager);

      joiner.joinerFuture.get();
   }


   public void testScenario_132_22() throws Exception {
      testScenario_13x_yx(2, 2);
   }

   public void testScenario_132_32() throws Exception {
      testScenario_13x_yx(2, 3);
   }

   public void testScenario_133_23() throws Exception {
      testScenario_13x_yx(3, 2);
   }

   public void testScenario_133_33() throws Exception {
      testScenario_13x_yx(3, 3);
   }

   protected void testScenario_13x_yx(int topologyOnNode0, int topologyOnNode2) throws Exception {
      assertTrue(2 <= topologyOnNode0 && topologyOnNode0 <= 3);
      assertTrue(2 <= topologyOnNode2 && topologyOnNode2 <= 3);
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = String.format("key_13%d_%d%d", topologyOnNode0, topologyOnNode2, topologyOnNode0);
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final BlockingLocalTopologyManager topologyManager1 = replaceTopologyManager(manager(1));
      final int currentTopologyId = currentTopologyId(cache(0));
      NewNode joiner = addNode(g -> TestCacheManagerFactory.addInterceptor(g, DEFAULT_CACHE_NAME::equals, new WaitForTopologyInterceptor(currentTopologyId + topologyOnNode2), TestCacheManagerFactory.InterceptorPosition.FIRST, null), null);

      // Install topology T1 everywhere
      confirmTopologyUpdate(Phase.READ_OLD_WRITE_ALL, topologyManager0, topologyManager1, joiner.topologyManager);

      //consistency check. the remote get is triggered
      awaitForTopology(currentTopologyId + 1, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      ControlledRpcManager.BlockedRequest blockedGet = rpcManager0.expectCommand(ClusteredGetCommand.class);

      // Install topology T2 everywhere
      confirmTopologyUpdate(Phase.READ_ALL_WRITE_ALL, topologyManager0, topologyManager1, joiner.topologyManager);

      // Install topology T3 where needed
      topologyManager1.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      if (topologyOnNode0 > 2) {
         topologyManager0.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }
      if (topologyOnNode2 > 2) {
         joiner.topologyManager.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }

      //wait until the consistent_hash_update arrives in old owner
      awaitForTopology(currentTopologyId + 3, cache(1));
      awaitForTopology(currentTopologyId + topologyOnNode0, cache(0));

      // Unblock the request and process the responses
      blockedGet.send().receiveAll();

      // Unblock the retry and its responses
      rpcManager0.expectCommand(ClusteredGetCommand.class).send().receiveAll();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + topologyOnNode0, cache(0));

      // Finish the rebalance
      if (topologyOnNode0 < 3) {
         topologyManager0.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }
      if (topologyOnNode2 < 3) {
         joiner.topologyManager.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }
      finishRebalance(Phase.NO_REBALANCE, topologyManager0, topologyManager1, joiner.topologyManager);

      joiner.joinerFuture.get();
   }


   public void testScenario_2112() throws Exception {
      testScenario_2xy2(1, 1, 1, 1);
   }

   public void testScenario_2212() throws Exception {
      testScenario_2xy2(2, 1, 1, 1);
   }

   public void testScenario_2122() throws Exception {
      testScenario_2xy2(1, 2, 2, -1);
   }

   public void testScenario_2132() throws Exception {
      testScenario_2xy2(1, 3, 2, -1);
   }

   public void testScenario_2222() throws Exception {
      testScenario_2xy2(2, 2, 2, -1);
   }

   public void testScenario_2232() throws Exception {
      testScenario_2xy2(2, 3, 2, -1);
   }

   public void testScenario_2322() throws Exception {
      testScenario_2xy2(3, 2, 1, 2);
   }

   public void testScenario_2332() throws Exception {
      testScenario_2xy2(3, 3, 1, 2);
   }

   protected void testScenario_2xy2(int topologyOnNode1, int topologyOnNode2, int expectedSuccessResponses,
                                    int expectSuccessFrom) throws Exception {
      assertTrue(1 <= topologyOnNode1 && topologyOnNode1 <= 3);
      assertTrue(1 <= topologyOnNode2 && topologyOnNode2 <= 3);
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = String.format("key_2%d%d2", topologyOnNode1, topologyOnNode2);
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final BlockingLocalTopologyManager topologyManager1 = replaceTopologyManager(manager(1));
      final int currentTopologyId = currentTopologyId(cache(0));

      extractInterceptorChain(cache(0))
            .addInterceptorAfter(new AssertNoRetryInterceptor(), StateTransferInterceptor.class);
      WaitForTopologyInterceptor wfti = new WaitForTopologyInterceptor(currentTopologyId + topologyOnNode2);
      NewNode joiner = addNode(g -> TestCacheManagerFactory.addInterceptor(g, DEFAULT_CACHE_NAME::equals, wfti, TestCacheManagerFactory.InterceptorPosition.FIRST, null), null);

      // Install topology T1 everywhere and T2 on the originator
      confirmTopologyUpdate(Phase.READ_OLD_WRITE_ALL, topologyManager0, topologyManager1, joiner.topologyManager);
      topologyManager0.confirmTopologyUpdate(Phase.READ_ALL_WRITE_ALL);
      awaitForTopology(currentTopologyId + 2, cache(0));

      // We wouldn't only need to install newer topologies on the new owner for the retry
      // but the coordinator will only start with a new topology after all the nodes confirmed the old one
      if (topologyOnNode1 > 1) {
         topologyManager1.confirmTopologyUpdate(Phase.READ_ALL_WRITE_ALL);
      }
      if (topologyOnNode2 > 1) {
         joiner.topologyManager.confirmTopologyUpdate(Phase.READ_ALL_WRITE_ALL);
      }
      if (topologyOnNode1 > 2) {
         topologyManager1.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }
      assertTopologyId(currentTopologyId + 2, cache(0));
      assertTopologyId(currentTopologyId + topologyOnNode1, cache(1));

      // Send the request and receive the response from node 1
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      ControlledRpcManager.SentRequest sentGet = rpcManager0.expectCommand(ClusteredGetCommand.class).send();
      sentGet.expectResponse(address(1));

      // Now we can install and confirm topology T2 on node 1
      // So that node 2 is free to install topology T3 if necessary
      if (topologyOnNode1 < 2) {
         topologyManager1.confirmTopologyUpdate(Phase.READ_ALL_WRITE_ALL);
      }
      if (topologyOnNode2 > 2) {
         joiner.topologyManager.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }
      eventuallyEquals(currentTopologyId + topologyOnNode2,
                       () -> wfti.distributionManager.getCacheTopology().getTopologyId());

      ControlledRpcManager.BlockedResponseMap blockedGet = sentGet.expectAllResponses();
      int succesful = 0;
      for (Map.Entry<Address, Response> rsp : blockedGet.getResponses().entrySet()) {
         if (rsp.getValue().isSuccessful()) {
            if (expectSuccessFrom >= 0) {
               assertEquals(address(expectSuccessFrom), rsp.getKey());
            }
            succesful++;
         } else {
            assertEquals(UnsureResponse.INSTANCE, rsp.getValue());
            if (expectSuccessFrom >= 0) {
               assertFalse(rsp.getKey().equals(address(expectSuccessFrom)));
            }
         }
      }
      assertTrue(succesful == expectedSuccessResponses);

      // Unblock the responses and retry if necessary
      blockedGet.receive();
      if (succesful == 0) {
         rpcManager0.expectCommand(ClusteredGetCommand.class).send().receiveAll();
      }

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());

      if (topologyOnNode2 < 2) {
         joiner.topologyManager.confirmTopologyUpdate(Phase.READ_ALL_WRITE_ALL);
      }
      topologyManager0.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      if (topologyOnNode1 < 3) {
         topologyManager1.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }
      if (topologyOnNode2 < 3) {
         joiner.topologyManager.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }
      finishRebalance(Phase.NO_REBALANCE, topologyManager0, topologyManager1, joiner.topologyManager);

      joiner.joinerFuture.get();
   }

   public void testScenario_2312_22() throws Exception {
      testScenario_2312_x2(2);
   }

   public void testScenario_2312_32() throws Exception {
      testScenario_2312_x2(3);
   }

   private void testScenario_2312_x2(int retryTopologyOnNode2) throws Exception {
      assertTrue(2 <= retryTopologyOnNode2 && retryTopologyOnNode2 <= 3);
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = String.format("key_2312_%d2", retryTopologyOnNode2);
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final BlockingLocalTopologyManager topologyManager1 = replaceTopologyManager(manager(1));
      final int currentTopologyId = currentTopologyId(cache(0));

      CyclicBarrier barrier1 = new CyclicBarrier(2);
      CyclicBarrier barrier2 = new CyclicBarrier(2);

      NewNode joiner = addNode(g -> TestCacheManagerFactory.addInterceptor(g, DEFAULT_CACHE_NAME::equals,
            new BlockingInterceptor<>(barrier2, GetCacheEntryCommand.class,true, false),
            TestCacheManagerFactory.InterceptorPosition.FIRST, null), null);

      // Install T1 everywhere and T2 on node 0
      confirmTopologyUpdate(Phase.READ_OLD_WRITE_ALL, topologyManager0, topologyManager1, joiner.topologyManager);
      topologyManager0.confirmTopologyUpdate(Phase.READ_ALL_WRITE_ALL);
      awaitForTopology(currentTopologyId + 2, cache(0));

      // Block the command on node 1 so we can install T3 first
      extractInterceptorChain(cache(1))
              .addInterceptor(new BlockingInterceptor<>(barrier1, GetCacheEntryCommand.class, false, false), 0);

      // Send the remote get and wait for the reply from node 2
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      ControlledRpcManager.SentRequest sentGet = rpcManager0.expectCommand(ClusteredGetCommand.class).send();
      barrier2.await(10, TimeUnit.SECONDS);
      barrier2.await(10, TimeUnit.SECONDS);
      sentGet.expectResponse(address(2), UnsureResponse.INSTANCE).receive();

      // Install T2 on nodes 1 and 2
      topologyManager1.confirmTopologyUpdate(Phase.READ_ALL_WRITE_ALL);
      joiner.topologyManager.confirmTopologyUpdate(Phase.READ_ALL_WRITE_ALL);

      // Now we can install T3 on node 1
      topologyManager1.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      awaitForTopology(currentTopologyId + 3, cache(1));

      // Also install T3 on node 2 if necessary
      if (retryTopologyOnNode2 > 2) {
         joiner.topologyManager.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }

      // Unblock read on node1 and receive the responses
      barrier1.await(10, TimeUnit.SECONDS);
      barrier1.await(10, TimeUnit.SECONDS);
      sentGet.expectResponse(address(1), UnsureResponse.INSTANCE).receive().finish();

      // Process retry
      ControlledRpcManager.SentRequest sentRetry = rpcManager0.expectCommand(ClusteredGetCommand.class).send();
      barrier1.await(10, TimeUnit.SECONDS);
      barrier1.await(10, TimeUnit.SECONDS);
      barrier2.await(10, TimeUnit.SECONDS);
      barrier2.await(10, TimeUnit.SECONDS);
      sentRetry.receiveAll();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 2, cache(0));

      // Finish the rebalance
      topologyManager0.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      if (retryTopologyOnNode2 < 3) {
         joiner.topologyManager.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }
      finishRebalance(Phase.NO_REBALANCE, topologyManager0, topologyManager1, joiner.topologyManager);

      joiner.joinerFuture.get();
   }

   public void testScenario_323() throws Exception {
      testScenario_xyx(3, 2);
   }

   public void testScenario_333() throws Exception {
      testScenario_xyx(3, 3);
   }

   public void testScenario_434() throws Exception {
      testScenario_xyx(4, 3);
   }

   protected void testScenario_xyx(int topologyOnNode0, int topologyOnNode2) throws Exception {
      assertTrue(3 <= topologyOnNode0 && topologyOnNode0 <= 4);
      assertTrue(2 <= topologyOnNode2 && topologyOnNode2 <= 3);
      assertTrue(topologyOnNode0 - topologyOnNode2 <= 1);
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = String.format("key_%d%d%d", topologyOnNode0, topologyOnNode2, topologyOnNode2);
      ownerCheckAndInit(cache(1), key, "v");
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final BlockingLocalTopologyManager topologyManager1 = replaceTopologyManager(manager(1));
      final int currentTopologyId = currentTopologyId(cache(0));

      extractInterceptorChain(cache(0))
            .addInterceptorAfter(new AssertNoRetryInterceptor(), StateTransferInterceptor.class);
      FailReadsInterceptor fri = new FailReadsInterceptor();
      extractInterceptorChain(cache(1)).addInterceptor(fri, 0);
      NewNode joiner = addNode(g -> TestCacheManagerFactory.addInterceptor(g, DEFAULT_CACHE_NAME::equals,
            new WaitForTopologyInterceptor(currentTopologyId + topologyOnNode2),
            TestCacheManagerFactory.InterceptorPosition.FIRST, null), null);

      // Install topology T2 everywhere
      confirmTopologyUpdate(Phase.READ_OLD_WRITE_ALL, topologyManager0, topologyManager1, joiner.topologyManager);
      confirmTopologyUpdate(Phase.READ_ALL_WRITE_ALL, topologyManager0, topologyManager1, joiner.topologyManager);

      // Install T3 on node 0 and the others as necessary
      topologyManager0.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      if (topologyOnNode2 > 2) {
         topologyManager1.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
         joiner.topologyManager.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }

      // Install T4 on node 0 if necessary
      if (topologyOnNode0 > 3) {
         topologyManager0.confirmTopologyUpdate(Phase.NO_REBALANCE);
      }
      awaitForTopology(currentTopologyId + topologyOnNode0, cache(0));

      // Send the remote get and check that it got a single response from node 2
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      fri.assertNotHit();

      assertTopologyId(currentTopologyId + topologyOnNode0, cache(0));

      if (topologyOnNode2 < 3) {
         topologyManager1.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
         joiner.topologyManager.confirmTopologyUpdate(Phase.READ_NEW_WRITE_ALL);
      }
      if (topologyOnNode0 < 4) {
         topologyManager0.confirmTopologyUpdate(Phase.NO_REBALANCE);
      }
      topologyManager1.confirmTopologyUpdate(Phase.NO_REBALANCE);
      joiner.topologyManager.confirmTopologyUpdate(Phase.NO_REBALANCE);

      joiner.joinerFuture.get();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, RemoteGetDuringStateTransferSCI.INSTANCE, configuration());
   }

   @Override
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
      NoOpGlobalConfigurationManager.amendCacheManager(cm);
   }

   private Future<Object> remoteGet(Cache cache, Object key) {
      return fork(() -> cache.get(key));
   }

   private int currentTopologyId(Cache cache) {
      return cache.getAdvancedCache().getDistributionManager().getCacheTopology().getTopologyId();
   }

   private void assertTopologyId(final int expectedTopologyId, final Cache cache) {
      assertEquals(expectedTopologyId, currentTopologyId(cache));
   }

   private void awaitForTopology(final int expectedTopologyId, final Cache cache) {
      eventuallyEquals(expectedTopologyId, () -> currentTopologyId(cache));
   }

   private void awaitUntilNotInDataContainer(final Cache cache, final Object key) {
      eventually(() -> !cache.getAdvancedCache().getDataContainer().containsKey(key));
   }

   private NewNode addNode(Consumer<GlobalConfigurationBuilder> modifyGlobal, Consumer<ConfigurationBuilder> modifyConfiguration) {
      NewNode newNode = new NewNode();
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.serialization().addContextInitializer(RemoteGetDuringStateTransferSCI.INSTANCE);
      if (modifyGlobal != null) {
         modifyGlobal.accept(global);
      }
      ConfigurationBuilder configurationBuilder = configuration();
      if (modifyConfiguration != null) {
         modifyConfiguration.accept(configurationBuilder);
      }
      EmbeddedCacheManager embeddedCacheManager = addClusterEnabledCacheManager(global, configurationBuilder);
      newNode.topologyManager = replaceTopologyManager(embeddedCacheManager);
      newNode.joinerFuture = fork(() -> {
         waitForClusterToForm();
         return null;
      });
      return newNode;
   }

   private void ownerCheckAndInit(Cache<Object, Object> owner, Object key, Object value) {
      assertTrue(address(owner) + " should be the owner of " + key + ".", isFirstOwner(owner, key));
      owner.put(key, value);
      assertCacheValue(key, value);
   }

   private void assertCacheValue(Object key, Object value) {
      for (Cache cache : caches()) {
         assertEquals("Wrong value for key " + key + " on " + address(cache) + ".", value, cache.get(key));
      }
   }

   private ConfigurationBuilder configuration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering()
             .hash()
             .numSegments(1)
             .numOwners(1)
             .stateTransfer()
             .timeout(30, TimeUnit.SECONDS);
      builder.addModule(PrivateCacheConfigurationBuilder.class).consistentHashFactory(new SingleKeyConsistentHashFactory());
      return builder;
   }

   private BlockingLocalTopologyManager replaceTopologyManager(EmbeddedCacheManager cacheContainer) {
      BlockingLocalTopologyManager localTopologyManager = BlockingLocalTopologyManager.replaceTopologyManagerDefaultCache(cacheContainer);
      topologyManagerList.add(localTopologyManager);
      return localTopologyManager;
   }

   private ControlledRpcManager replaceRpcManager(Cache cache) {
      ControlledRpcManager controlledRpcManager = ControlledRpcManager.replaceRpcManager(cache);
      rpcManagerList.add(controlledRpcManager);
      return controlledRpcManager;
   }

   @ProtoName("RemoteGetSingleKeyConsistentHashFactory")
   public static class SingleKeyConsistentHashFactory extends BaseControlledConsistentHashFactory.Default {

      SingleKeyConsistentHashFactory() {
         super(1);
      }

      @Override
      protected int[][] assignOwners(int numSegments, List<Address> members) {
         return new int[][]{{members.size() - 1}};
      }
   }

   static class WaitForTopologyInterceptor extends DDAsyncInterceptor {
      private static final Log log = LogFactory.getLog(RemoteGetDuringStateTransferTest.class);

      protected final int expectedTopologyId;
      // ugly hooks to be able to access topology from test
      private volatile DistributionManager distributionManager;
      private volatile StateTransferLock stateTransferLock;

      private WaitForTopologyInterceptor(int expectedTopologyId) {
         this.expectedTopologyId = expectedTopologyId;
      }

      @Inject
      public void init(DistributionManager distributionManager, StateTransferLock stateTransferLock) {
         this.distributionManager = distributionManager;
         this.stateTransferLock = stateTransferLock;
      }

      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
         assertNotNull(stateTransferLock);
         log.tracef("Waiting for topology %d before executing %s", expectedTopologyId, command);
         stateTransferLock.topologyFuture(expectedTopologyId).toCompletableFuture().get(10, TimeUnit.SECONDS);
         assertEquals(expectedTopologyId, distributionManager.getCacheTopology().getTopologyId());
         return invokeNext(ctx, command);
      }
   }

   static class FailReadsInterceptor extends BaseCustomAsyncInterceptor {
      private final AtomicBoolean hit = new AtomicBoolean();

      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
         hit.set(true);
         throw new IllegalStateException("Did not expect the command to be executed on node " + cache.getCacheManager().getAddress());
      }

      public void assertNotHit() {
         assertFalse(hit.get());
      }
   }

   static class AssertNoRetryInterceptor extends DDAsyncInterceptor {
      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) {
         assertFalse(command.hasAnyFlag(FlagBitSets.COMMAND_RETRY));
         return invokeNextAndExceptionally(ctx, command, (rCtx, rCommand, t) -> {
            assertFalse(t instanceof OutdatedTopologyException);
            throw  t;
         });
      }
   }

   private static class NewNode {
      Future<Void> joinerFuture;
      BlockingLocalTopologyManager topologyManager;
   }

   @ProtoSchema(
         includeClasses = SingleKeyConsistentHashFactory.class,
         schemaFileName = "test.core.RemoteGetDuringStateTransferTest.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.RemoteGetDuringStateTransferTest",
         service = false
   )
   interface RemoteGetDuringStateTransferSCI extends SerializationContextInitializer {
      RemoteGetDuringStateTransferSCI INSTANCE = new RemoteGetDuringStateTransferSCIImpl();
   }
}
