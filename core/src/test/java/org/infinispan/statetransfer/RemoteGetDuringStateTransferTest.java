package org.infinispan.statetransfer;

import static org.infinispan.distribution.DistributionTestHelper.isFirstOwner;
import static org.infinispan.util.BlockingLocalTopologyManager.LatchType;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.infinispan.util.BlockingLocalTopologyManager;
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
         Collections.synchronizedList(new ArrayList<BlockingLocalTopologyManager>(4));
   private final List<ControlledRpcManager> rpcManagerList =
         Collections.synchronizedList(new ArrayList<ControlledRpcManager>(4));

   /*
   Summary

   T0   initial topology
   T1   state transfer started
   T2   state transfer finished but rebalance not complete
   T3   read new, write all topology
   T4   rebalance completed

   | sc     | osc | first request | process request 1 | receive response 1 | retry | process request 2 | receive response 2 |
   | 010    | 1   | T0            | 1:T1              | T0                 | N1    |                   |                    |
   | 011    | 2   | T0            | 1:T1              | T1/T2/T3/T4        | N1    |                   |                    |
   | [2]    |     | T0            | 1:T2              | T0                 |       |                   |                    |
   | [1]    |     | T0            | 1:T2              | T1/T2/T3/T4        | N1    |                   |                    |
   | [2]    |     | T0            | 1:T3              | T0/T1              |       |                   |                    |
   | [2]    |     | T0            | 1:T3              | T2                 | Y*    | 2:T0/T1           |                    |
   | [4]    |     | T0            | 1:T3              | T2                 | Y*    | 2:T2              | T0/T1              |
   | 032_22 | 3.1 | T0            | 1:T3              | T2                 | Y*    | 2:T2              | T2/T3/T4           |
   | 032_32 | 3.2 | T0            | 1:T3              | T2                 | Y*    | 2:T3/T4           | T2/T3/T4           |
   | [2]    |     | T0            | 1:T3              | T3                 | Y     | 2:T0/T1           |                    |
   | [4]    |     | T0            | 1:T3              | T3                 | Y     | 2:T2              | T0/T1/T2           |
   | 033_23 | 4.1 | T0            | 1:T3              | T3                 | Y     | 2:T2              | T3/T4              |
   | [4]    |     | T0            | 1:T3              | T3                 | Y     | 2:T3/T4           | T0/T1/T2           |
   | 033_33 | 4.2 | T0            | 1:T3              | T3                 | Y     | 2:T3/T4           | T3/T4              |
   | [2]    |     | T0            | 1:T3              | T4                 | Y     | 2:T0/T1/T2        |                    |
   | [4]    |     | T0            | 1:T3              | T4                 | Y     | 2:T3/T4           | T0/T1/T2/T3        |
   | [1]    |     | T0            | 1:T3              | T4                 | Y     | 2:T3/T4           | T4                 |
   | [2]    |     | T0            | 1:T4              | T0/T1/T2           |       |                   |                    |
   | [4]    |     | T0            | 1:T4              | T3/T4              | Y     | 2:T0/T1/T2        |                    |
   | [2]    |     | T0            | 1:T4              | T3/T4              | Y     | 2:T3/T4           | T0/T1/T2           |
   | [1]    |     | T0            | 1:T4              | T3/T4              | Y     | 2:T3/T4           | T3/T4              |
   | [4]    |     | T1            | 1:T0              | T0                 |       |                   |                    |
   | 101    | 5   | T1            | 1:T0              | T1/T2/T3/T4        | N1    |                   |                    |
   | [4]    |     | T1            | 1:T1              | T0                 |       |                   |                    |
   | 111    |     | T1            | 1:T1              | T1/T2/T3/T4        |       |                   |                    |
   | [4]    |     | T1            | 1:T2              | T0                 |       |                   |                    |
   | [1]    |     | T1            | 1:T2              | T1/T2/T3/T4        | N1    |                   |                    |
   | [2]    |     | T1            | 1:T3              | T2                 | Y*    | 2:T0/T1           |                    |
   | [4]    |     | T1            | 1:T3              | T2                 | Y*    | 2:T2              | T0/T1              |
   | 132_22 | 6.1 | T1            | 1:T3              | T2                 | Y*    | 2:T2              | T2/T3/T4           |
   | [4]    |     | T1            | 1:T3              | T2                 | Y*    | 2:T3              | T0/T1              |
   | 132_32 | 6.2 | T1            | 1:T3              | T2                 | Y*    | 2:T3              | T2/T3/T4           |
   | [2]    |     | T1            | 1:T3              | T2                 | Y*    | 2:T4              | T0/T1/T2           |
   | [1]    |     | T1            | 1:T3              | T2                 | Y*    | 2:T4              | T3/T4              |
   | [2]    |     | T1            | 1:T3              | T3                 | Y     | 2:T0/T1           |                    |
   | [4]    |     | T1            | 1:T3              | T3                 | Y     | 2:T2              | T0/T1/T2           |
   | 133_23 | 7.1 | T1            | 1:T3              | T3                 | Y     | 2:T2              | T3/T4              |
   | [4]    |     | T1            | 1:T3              | T3                 | Y     | 2:T3              | T0/T1/T2           |
   | 133_33 | 7.2 | T1            | 1:T3              | T3                 | Y     | 2:T3              | T3/T4              |
   | [4]    |     | T1            | 1:T3              | T3                 | Y     | 2:T4              | T0/T1/T2           |
   | [1]    |     | T1            | 1:T3              | T3                 | Y     | 2:T4              | T3/T4              |
   | [2]    |     | T1            | 1:T3              | T4                 | Y     | 2:T0/T1/T2        |                    |
   | [1]    |     | T1            | 1:T3              | T4                 | Y     | 2:T3/T4           | T4                 |
   | [4]    |     | T1            | 1:T4              | T0/T1/T2           |       |                   |                    |
   | [2]    |     | T1            | 1:T4              | T3/T4              | Y     | 2:T0/T1/T2        |                    |
   | [4]    |     | T1            | 1:T4              | T3/T4              | Y     | 2:T3/T4           | T0/T1/T2           |
   | [1]    |     | T1            | 1:T4              | T3/T4              | Y     | 2:T3/T4           | T3/T4              |
   | [2]    |     | T2            | 1:T0              |                    |       |                   |                    |
   | [4]    |     | T2            | 1: *, 2:  *       | T0/T1              |       |                   |                    |
   | 2112   | 8.1 | T2            | 1:T1, 2: T1       | T2/T3/T4           | N1    |                   |                    |
   | 2122   | 8.2 | T2            | 1:T1, 2: T2       | T2/T3/T4           | N1+2  |                   |                    |
   | 2132   | 8.3 | T2            | 1:T1, 2: T3/T4    | T2/T3/T4           | N1+2  |                   |                    |
   | 2212   | 8.4 | T2            | 1:T2, 2: T1       | T2/T3/T4           | N1    |                   |                    |
   | 2222   | 8.5 | T2            | 1:T2, 2: T2       | T2/T3/T4           | N1+2  |                   |                    |
   | 2232   | 8.6 | T2            | 1:T2, 2: T3/T4    | T2/T3/T4           | N1+2  |                   |                    |
   | 2312_22| 9.1 | T2            | 1:T3/T4, 2: T1    | T2/T3/T4           | Y     | 2: T2             | T2/T3/T4           |
   | 2312_32| 9.2 | T2            | 1:T3/T4, 2: T1    | T2/T3/T4           | Y     | 2: T3/T4          | T2/T3/T4           |
   | 2322   | 8.7 | T2            | 1:T3/T4, 2: T2    | T2/T3/T4           | N2    |                   |                    |
   | 2332   | 8.8 | T2            | 1:T3/T4, 2: T3/T4 | T2/T3/T4           | N2    |                   |                    |
   | [2]    |     | T3            | 2: T0/T1          |                    |       |                   |                    |
   | [4]    |     | T3            | 2: T2             | T0/T1/T2           |       |                   |                    |
   | 323    | 10  | T3            | 2: T2             | T3/T4              | N2    |                   |                    |
   | [4]    |     | T3            | 2: T3/T4          | T0/T1/T2           |       |                   |                    |
   | 333    | 11  | T3            | 2: T3/T4          | T3/T4              | N2    |                   |                    |
   | [2]    |     | T4            | 2: T0/T1/T2       |                    |       |                   |                    |
   | [4]    | 12  | T4            | 2:T3              | T0/T1/T2/T3        | N2    |                   |                    |
   | 434    | 12  | T4            | 2:T3              | T4                 | N2    |                   |                    |

   *) The retry will go to both node 1 and 2 but 1 in T3 will respond with UnsureResponse
   [1] too similar to the previous scenario
   [2] impossible because we topologies can't differ by more than 1 at the same time
   [4] impossible, first response was received in later topology than second response

   A note for 2312_x2: while the two nodes cannot have topologies 3 and 1 at the same time, the two reads can arrive
   at different times there.
    */

   @AfterMethod(alwaysRun = true)
   public final void unblockAll() {
      //keep track of all controlled components. In case of failure, we need to unblock all otherwise we have to wait
      //long time until the test is able to stop all cache managers.
      for (BlockingLocalTopologyManager topologyManager : topologyManagerList) {
         topologyManager.stopBlockingAll();
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
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager0.blockBefore(ClusteredGetCommand.class);
      topologyManager0.startBlocking(LatchType.REBALANCE);

      cache(0).getAdvancedCache().getAsyncInterceptorChain()
            .addInterceptorAfter(new AssertNoRetryInterceptor(), StateTransferInterceptor.class);

      //remote get is processed in current topology id.
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager0.waitForCommandToBlock();

      NewNode joiner = addNode(null, cb -> cb.customInterceptors().addInterceptor()
         .position(InterceptorConfiguration.Position.FIRST).interceptor(new FailReadsInterceptor()));

      topologyManager0.waitToBlock(LatchType.REBALANCE);

      //wait until the rebalance_start arrives in old owner and let the remote get go
      awaitForTopology(currentTopologyId + 1, cache(1));
      rpcManager0.stopBlocking();

      //check the value returned and make sure that the requestor is still in currentTopologyId (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId, cache(0));

      topologyManager0.stopBlocking(LatchType.REBALANCE);
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
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager0.blockBefore(ClusteredGetCommand.class);
      topologyManager0.startBlocking(LatchType.CONFIRM_REBALANCE_PHASE);

      cache(0).getAdvancedCache().getAsyncInterceptorChain()
            .addInterceptorAfter(new AssertNoRetryInterceptor(), StateTransferInterceptor.class);

      //the remote get is triggered in the current topology id.
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager0.waitForCommandToBlock();

      NewNode joiner = addNode(null, cb -> cb.customInterceptors().addInterceptor()
            .position(InterceptorConfiguration.Position.FIRST).interceptor(new FailReadsInterceptor()));
      topologyManager0.waitToBlock(LatchType.CONFIRM_REBALANCE_PHASE);

      //wait until the rebalance start arrives in old owner and in the requestor. then let the remote get go.
      awaitForTopology(currentTopologyId + 1, cache(1));
      awaitForTopology(currentTopologyId + 1, cache(0));
      rpcManager0.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(1));
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager0.stopBlocking(LatchType.CONFIRM_REBALANCE_PHASE);
      joiner.joinerFuture.get();
   }

   public void testScenario_101() throws Exception {
      testScenario_1x1(0);
   }

   public void testScenario_111() throws Exception {
      testScenario_1x1(1);
   }

   protected void testScenario_1x1(int topologyOnNode1) throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = String.format("key_1%d1", topologyOnNode1);
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final BlockingLocalTopologyManager topologyManager1 = replaceTopologyManager(manager(1));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager0.blockBefore(ClusteredGetCommand.class);
      topologyManager0.startBlocking(LatchType.CONFIRM_REBALANCE_PHASE);
      if (topologyOnNode1 == 0) {
         topologyManager1.startBlocking(LatchType.REBALANCE);
      }

      cache(0).getAdvancedCache().getAsyncInterceptorChain()
            .addInterceptorAfter(new AssertNoRetryInterceptor(), StateTransferInterceptor.class);

      NewNode joiner = addNode(null, cb -> cb.customInterceptors().addInterceptor()
            .position(InterceptorConfiguration.Position.FIRST).interceptor(new FailReadsInterceptor()));

      //consistency check
      awaitForTopology(currentTopologyId + 1, cache(0));

      //the remote get is triggered after the rebalance_start and before the confirm_rebalance.
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager0.waitForCommandToBlock();

      //wait until the rebalance_start arrives in old owner
      awaitForTopology(currentTopologyId + topologyOnNode1, cache(1));
      rpcManager0.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager1.stopBlocking(LatchType.REBALANCE);
      topologyManager0.stopBlocking(LatchType.CONFIRM_REBALANCE_PHASE);
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
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = String.format("key_03%d_%d%d", topologyOnNode0, topologyOnNode2, topologyOnNode0);
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager0.blockBefore(ClusteredGetCommand.class);
      // allow read_old -> read_all but not read_all -> read_new
      topologyManager0.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);

      //consistency check. the remote get is triggered
      assertTopologyId(currentTopologyId, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager0.waitForCommandToBlock();

      NewNode joiner = addNode(LatchType.CONSISTENT_HASH_UPDATE, cb -> cb.customInterceptors().addInterceptor()
            .position(InterceptorConfiguration.Position.FIRST)
            .interceptor(new WaitForTopologyInterceptor(currentTopologyId + topologyOnNode2)));
      joiner.localTopologyManager.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
      if (topologyOnNode2 > 2) {
         joiner.localTopologyManager.waitToBlockAndUnblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
      }

      //wait until the consistent_hash_update arrives in old owner
      awaitForTopology(currentTopologyId + 3, cache(1));
      awaitForTopology(currentTopologyId + 2, cache(0));
      if (topologyOnNode0 > 2) {
         topologyManager0.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
         awaitForTopology(currentTopologyId + 3, cache(0));
      }
      rpcManager0.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + topologyOnNode0, cache(0));

      topologyManager0.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.localTopologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);

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
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = String.format("key_13%d_%d%d", topologyOnNode0, topologyOnNode2, topologyOnNode0);
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      topologyManager0.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      rpcManager0.blockBefore(ClusteredGetCommand.class);

      NewNode joiner = addNode(LatchType.CONSISTENT_HASH_UPDATE, cb -> cb.customInterceptors().addInterceptor()
            .position(InterceptorConfiguration.Position.FIRST)
            .interceptor(new WaitForTopologyInterceptor(currentTopologyId + topologyOnNode2)));

      //consistency check. the remote get is triggered
      awaitForTopology(currentTopologyId + 1, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager0.waitForCommandToBlock();

      // allow read_old -> read_all but not read_all -> read_new
      topologyManager0.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);

      joiner.localTopologyManager.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
      if (topologyOnNode2 > 2) {
         joiner.localTopologyManager.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
      }

      //wait until the consistent_hash_update arrives in old owner
      awaitForTopology(currentTopologyId + 3, cache(1));
      awaitForTopology(currentTopologyId + 2, cache(0));
      if (topologyOnNode0 > 2) {
         topologyManager0.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
         awaitForTopology(currentTopologyId + 3, cache(0));
      }
      rpcManager0.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + topologyOnNode0, cache(0));

      topologyManager0.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.localTopologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);

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

   protected void testScenario_2xy2(int topologyOnNode1, int topologyOnNode2, int expectedSuccessResponses, int expectSuccessFrom) throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = String.format("key_2%d%d2", topologyOnNode1, topologyOnNode2);
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final BlockingLocalTopologyManager topologyManager1 = replaceTopologyManager(manager(1));
      final int currentTopologyId = currentTopologyId(cache(0));

      topologyManager0.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      topologyManager1.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      cache(0).getAdvancedCache().getAsyncInterceptorChain()
            .addInterceptorAfter(new AssertNoRetryInterceptor(), StateTransferInterceptor.class);
      WaitForTopologyInterceptor wfti = new WaitForTopologyInterceptor(currentTopologyId + topologyOnNode2);
      NewNode joiner = addNode(LatchType.CONSISTENT_HASH_UPDATE, cb -> cb.customInterceptors().addInterceptor()
               .position(InterceptorConfiguration.Position.FIRST).interceptor(wfti));

      // allow read_old -> read_all but not read_all -> read_new
      joiner.localTopologyManager.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
      if (topologyOnNode2 > 2) {
         joiner.localTopologyManager.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
      }
      topologyManager0.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
      for (int i = 1; i < topologyOnNode1; ++i) {
         topologyManager1.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
      }
      awaitForTopology(currentTopologyId + 2, cache(0));
      awaitForTopology(currentTopologyId + topologyOnNode1, cache(1));

      CyclicBarrier barrier1 = new CyclicBarrier(2);
      cache(1).getAdvancedCache().getAsyncInterceptorChain()
            .addInterceptor(new BlockingInterceptor(barrier1, GetCacheEntryCommand.class, true, false), 0);

      // TODO: add more determinism by waiting for all responses
      rpcManager0.blockAfter(ClusteredGetCommand.class);
      rpcManager0.checkResponses(responseMap -> {
         int succesful = 0;
         for (Map.Entry<Address, Response> rsp : responseMap.entrySet()) {
            if (rsp.getValue().isSuccessful()) {
               if (expectSuccessFrom >= 0) {
                  assertEquals(cacheManagers.get(expectSuccessFrom).getAddress(), rsp.getKey());
               }
               succesful++;
            } else {
               assertEquals(UnsureResponse.INSTANCE, rsp.getValue());
               if (expectSuccessFrom >= 0) {
                  assertFalse(rsp.getKey().equals(cacheManagers.get(expectSuccessFrom).getAddress()));
               }
            }
         }
         assertTrue(succesful <= expectedSuccessResponses);
      });
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);

      assertTopologyId(currentTopologyId + 2, cache(0));
      assertTopologyId(currentTopologyId + topologyOnNode1, cache(1));

      barrier1.await(10, TimeUnit.SECONDS);
      topologyManager1.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      eventually(() -> wfti.stateTransferManager.getCacheTopology().getTopologyId() >= currentTopologyId + topologyOnNode2);
      barrier1.await(10, TimeUnit.SECONDS);

      rpcManager0.waitForCommandToBlock();
      rpcManager0.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());

      topologyManager0.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.localTopologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      joiner.joinerFuture.get();
   }

   public void testScenario_2312_22() throws Exception {
      testScenario_2312_x2(2);
   }

   public void testScenario_2312_32() throws Exception {
      testScenario_2312_x2(3);
   }

   protected void testScenario_2312_x2(int retryTopologyOnNode2) throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = String.format("key_2312_%d2", retryTopologyOnNode2);
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final BlockingLocalTopologyManager topologyManager1 = replaceTopologyManager(manager(1));
      final int currentTopologyId = currentTopologyId(cache(0));

      topologyManager0.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      topologyManager1.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      CyclicBarrier barrier1 = new CyclicBarrier(2);
      CyclicBarrier barrier2 = new CyclicBarrier(2);

      NewNode joiner = addNode(LatchType.CONSISTENT_HASH_UPDATE, cb -> cb.customInterceptors().addInterceptor()
            .position(InterceptorConfiguration.Position.FIRST)
            .interceptor(new BlockingInterceptor(barrier2, GetCacheEntryCommand.class, true, false)));

      // allow node0 up to T2 and node1 up to T3
      topologyManager0.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
      for (int i = 1; i < 3; ++i) {
         topologyManager1.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
      }
      awaitForTopology(currentTopologyId + 2, cache(0));

      cache(1).getAdvancedCache().getAsyncInterceptorChain()
            .addInterceptor(new BlockingInterceptor(barrier1, GetCacheEntryCommand.class, false, false), 0);

      rpcManager0.blockAfter(ClusteredGetCommand.class);
      rpcManager0.checkResponses(responseMap -> {
         assertEquals(responseMap.toString(), 2, responseMap.size());
         for (Map.Entry<Address, Response> rsp : responseMap.entrySet()) {
            assertEquals(UnsureResponse.INSTANCE, rsp.getValue());
         }
      });
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);

      // wait for read on node2
      barrier2.await(10, TimeUnit.SECONDS);
      barrier2.await(10, TimeUnit.SECONDS);
      // unblock state transfer on node2, that should allow node1 to progress
      for (int i = 1; i < retryTopologyOnNode2; ++i) {
         joiner.localTopologyManager.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
      }
      awaitForTopology(currentTopologyId + 3, cache(1));
      // unblock read on node1
      barrier1.await(10, TimeUnit.SECONDS);
      barrier1.await(10, TimeUnit.SECONDS);

      rpcManager0.waitForCommandToBlock();
      rpcManager0.stopBlocking();

      // release retry on joiner
      barrier2.await(10, TimeUnit.SECONDS);
      barrier2.await(10, TimeUnit.SECONDS);

      assertTopologyId(currentTopologyId + 2, cache(0));

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());

      topologyManager0.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      topologyManager1.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.localTopologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);

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
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = String.format("key_%d%d%d", topologyOnNode0, topologyOnNode2, topologyOnNode2);
      ownerCheckAndInit(cache(1), key, "v");
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      topologyManager0.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      cache(0).getAdvancedCache().getAsyncInterceptorChain()
            .addInterceptorAfter(new AssertNoRetryInterceptor(), StateTransferInterceptor.class);
      cache(1).getAdvancedCache().getAsyncInterceptorChain()
            .addInterceptor(new FailReadsInterceptor(), 0);

      NewNode joiner = addNode(LatchType.CONSISTENT_HASH_UPDATE, cb -> cb.customInterceptors().addInterceptor()
            .position(InterceptorConfiguration.Position.FIRST)
            .interceptor(new WaitForTopologyInterceptor(currentTopologyId + topologyOnNode2)));

      for (int i = 1; i < topologyOnNode0; ++i) {
         topologyManager0.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
      }
      for (int i = 1; i < topologyOnNode2; ++i) {
         joiner.localTopologyManager.unblockOnce(LatchType.CONSISTENT_HASH_UPDATE);
      }
      awaitForTopology(currentTopologyId + topologyOnNode0, cache(0));

      Future<Object> remoteGetFuture = remoteGet(cache(0), key);

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());

      assertTopologyId(currentTopologyId + topologyOnNode0, cache(0));
      topologyManager0.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.localTopologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      joiner.joinerFuture.get();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, configuration());
   }

   private Future<Object> remoteGet(Cache cache, Object key) {
      return fork(() -> cache.get(key));
   }

   private int currentTopologyId(Cache cache) {
      return TestingUtil.extractComponent(cache, StateTransferManager.class).getCacheTopology().getTopologyId();
   }

   private void assertTopologyId(final int expectedTopologyId, final Cache cache) {
      assertEquals(expectedTopologyId, currentTopologyId(cache));
   }

   private void awaitForTopology(final int expectedTopologyId, final Cache cache) {
      eventually(() -> {
         int currentTopologyId = currentTopologyId(cache);
         assertTrue("Current topology is " + currentTopologyId, currentTopologyId <= expectedTopologyId);
         return expectedTopologyId == currentTopologyId;
      });
   }

   private void awaitUntilNotInDataContainer(final Cache cache, final Object key) {
      eventually(() -> !cache.getAdvancedCache().getDataContainer().containsKey(key));
   }

   private NewNode addNode() {
      return addNode(null, null);
   }

   private NewNode addNode(LatchType block, Consumer<ConfigurationBuilder> modifyConfiguration) {
      NewNode newNode = new NewNode();
      ConfigurationBuilder configurationBuilder = configuration();
      if (modifyConfiguration != null) {
         modifyConfiguration.accept(configurationBuilder);
      }
      EmbeddedCacheManager embeddedCacheManager = addClusterEnabledCacheManager(configurationBuilder);
      newNode.localTopologyManager = replaceTopologyManager(embeddedCacheManager);
      if (block != null) {
         newNode.localTopologyManager.startBlocking(block);
      }
      topologyManagerList.add(newNode.localTopologyManager);
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
            .consistentHashFactory(new SingleKeyConsistentHashFactory())
            .stateTransfer()
            .fetchInMemoryState(true);
      return builder;
   }

   private BlockingLocalTopologyManager replaceTopologyManager(CacheContainer cacheContainer) {
      BlockingLocalTopologyManager localTopologyManager = BlockingLocalTopologyManager.replaceTopologyManager(cacheContainer);
      topologyManagerList.add(localTopologyManager);
      return localTopologyManager;
   }

   private ControlledRpcManager replaceRpcManager(Cache cache) {
      RpcManager manager = TestingUtil.extractComponent(cache, RpcManager.class);
      ControlledRpcManager controlledRpcManager = new ControlledRpcManager(manager);
      TestingUtil.replaceComponent(cache, RpcManager.class, controlledRpcManager, true);
      rpcManagerList.add(controlledRpcManager);
      return controlledRpcManager;
   }

   private <T extends StateTransferLock> T replaceStateTransferLock(Cache cache, Function<StateTransferLock, T> lockBuilder) {
      StateTransferLock lock = TestingUtil.extractComponent(cache, StateTransferLock.class);
      T controlledLock = lockBuilder.apply(lock);
      TestingUtil.replaceComponent(cache, StateTransferLock.class, controlledLock, true);
      return controlledLock;
   }

   @SuppressWarnings("unchecked")
   public static class SingleKeyConsistentHashFactory extends BaseControlledConsistentHashFactory {

      public SingleKeyConsistentHashFactory() {
         super(1);
      }

      @Override
      protected List<Address> createOwnersCollection(List<Address> members, int numberOfOwners, int segmentIndex) {
         assertEquals("Wrong number of owners.", 1, numberOfOwners);
         return Collections.singletonList(members.get(members.size() - 1));
      }
   }

   private static class WaitForTopologyInterceptor extends DDAsyncInterceptor {
      protected final int expectedTopologyId;
      // ugly hooks to be able to access topology from test
      private volatile StateTransferManager stateTransferManager;
      private volatile StateTransferLock stateTransferLock;

      private WaitForTopologyInterceptor(int expectedTopologyId) {
         this.expectedTopologyId = expectedTopologyId;
      }

      @Inject
      public void init(StateTransferManager stateTransferManager, StateTransferLock stateTransferLock) {
         this.stateTransferManager = stateTransferManager;
         this.stateTransferLock = stateTransferLock;
      }

      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
         assertNotNull(stateTransferLock);
         CompletableFuture<Void> topologyFuture = stateTransferLock.topologyFuture(expectedTopologyId);
         if (topologyFuture != null) {
            topologyFuture.get(10, TimeUnit.SECONDS);
         }
         assertEquals(expectedTopologyId, stateTransferManager.getCacheTopology().getTopologyId());
         return invokeNext(ctx, command);
      }
   }

   private static class FailReadsInterceptor extends BaseCustomAsyncInterceptor {
      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
         throw new IllegalStateException("Did not expect the command to be executed on node " + cache.getCacheManager().getAddress());
      }
   }

   private static class AssertNoRetryInterceptor extends DDAsyncInterceptor {
      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
         assertFalse(command.hasFlag(Flag.COMMAND_RETRY));
         return invokeNextAndExceptionally(ctx, command, (rCtx, rCommand, t) -> {
            assertFalse(t instanceof OutdatedTopologyException);
            throw  t;
         });
      }
   }

   private class NewNode {
      Future<Void> joinerFuture;
      BlockingLocalTopologyManager localTopologyManager;
   }
}
