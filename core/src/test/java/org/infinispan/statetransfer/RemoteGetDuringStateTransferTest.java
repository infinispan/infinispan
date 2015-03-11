package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.infinispan.distribution.DistributionTestHelper.isFirstOwner;
import static org.infinispan.util.BlockingLocalTopologyManager.LatchType;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Test multiple possible situations of interleaving between a remote get and state transfer.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "unstable", testName = "statetransfer.RemoteGetDuringStateTransferTest")
@CleanupAfterMethod
public class RemoteGetDuringStateTransferTest extends MultipleCacheManagersTest {

   private final List<BlockingLocalTopologyManager> topologyManagerList =
         Collections.synchronizedList(new ArrayList<BlockingLocalTopologyManager>(4));
   private final List<ControlledRpcManager> rpcManagerList =
         Collections.synchronizedList(new ArrayList<ControlledRpcManager>(4));

   /*
   Summary

   T = initial topology, T1  = rebalance started but we have no data yet, T1.5 rebalance started and we have data, T2  = rebalance finished

   The originator will retry the remote get if node 1 processes the request in T2.
   If node 1 processes the request in T0 or T1, it still has the entry, and the originator doesn't have to retry.

   | sc     | osc | first request | process request 1 | receive response 1 | retry | process request 2 | receive response 2 |
   | 010    | 1   | T0            | 1:T1              | T0                 | N     |                   |                    |
   | 011    | 2   | T0            | 1:T1              | T1                 | N     |                   |                    |
   | [1]    |     | T0            | 1:T1              | T2                 | N     |                   |                    |
   | [2]    |     | T0            | 1:T2              | T0                 | N/A   |                   |                    |
   | [2]    |     | T0            | 1:T2              | T1                 | Y     | 2:T0              | N/A                |
   | 021_11 | 4   | T0            | 1:T2              | T1                 | Y     | 2:T1              | T1                 |
   | [1]    |     | T0            | 1:T2              | T1                 | Y     | 2:T1              | T2                 |
   | 021_21 | 4.1 | T0            | 1:T2              | T1                 | Y     | 2:T2              | T1                 |
   | [1]    |     | T0            | 1:T2              | T1                 | Y     | 2:T2              | T2                 |
   | 022_11 | 5   | T0            | 1:T2              | T2                 | Y     | 2:T1              | T1                 |
   | [1]    |     | T0            | 1:T2              | T2                 | Y     | 2:T1              | T1                 |
   | 022_22 | 5.1 | T0            | 1:T2              | T2                 | Y     | 2:T2              | T2                 |
   | 111    | 3   | T1            | 1:T1              | T1                 | N     |                   |                    |
   | [1]    |     | T1            | 1:T1              | T2                 | N     |                   |                    |
   | 121_11 | 6   | T1            | 1:T2              | T1                 | Y     | 2:T1              | T1                 |
   | [1]    |     | T1            | 1:T2              | T1                 | Y     | 2:T1              | T2                 |
   | 121_21 | 6.1 | T1            | 1:T2              | T1                 | Y     | 2:T2              | T1                 |
   | [1]    |     | T1            | 1:T2              | T1                 | Y     | 2:T2              | T2                 |
   | 122_11 | 7   | T1            | 1:T2              | T2                 | Y     | 2:T1              | T1                 |
   | [1]    | 7   | T1            | 1:T2              | T2                 | Y     | 2:T1              | T1                 |
   | 122_22 | 7.1 | T1            | 1:T2              | T2                 | Y     | 2:T2              | T2                 |

   [1] too similar to the previous scenario
   [2] impossible because we can't have T0 on one node and T2 on another node at the same time
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
      final Object key = "key_s1";
      ownerCheckAndInit(cache(1), key, "v");

      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager0.blockBefore(ClusteredGetCommand.class);
      topologyManager0.startBlocking(LatchType.REBALANCE);

      //remote get is processed in current topology id.
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager0.waitForCommandToBlock();

      NewNode joiner = addNode();
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
      final Object key = "key_s2";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager0.blockBefore(ClusteredGetCommand.class);
      topologyManager0.startBlocking(LatchType.CONFIRM_REBALANCE);

      //the remote get is triggered in the current topology id.
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager0.waitForCommandToBlock();

      NewNode joiner = addNode();
      topologyManager0.waitToBlock(LatchType.CONFIRM_REBALANCE);

      //wait until the rebalance start arrives in old owner and in the requestor. then let the remote get go.
      awaitForTopology(currentTopologyId + 1, cache(1));
      awaitForTopology(currentTopologyId + 1, cache(0));
      rpcManager0.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager0.stopBlocking(LatchType.CONFIRM_REBALANCE);
      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3315: the remote get is triggered and the reply received after the rebalance_start command. As in previous
    * scenario, the old owner receives the request after the rebalance_start command.
    */
   public void testScenario_111() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s3";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager0.blockBefore(ClusteredGetCommand.class);
      topologyManager0.startBlocking(LatchType.CONFIRM_REBALANCE);

      NewNode joiner = addNode();
      topologyManager0.waitToBlock(LatchType.CONFIRM_REBALANCE);

      //consistency check
      awaitForTopology(currentTopologyId + 1, cache(0));

      //the remote get is triggered after the rebalance_start and before the confirm_rebalance.
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager0.waitForCommandToBlock();

      //wait until the rebalance_start arrives in old owner
      awaitForTopology(currentTopologyId + 1, cache(1));
      rpcManager0.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager0.stopBlocking(LatchType.CONFIRM_REBALANCE);
      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3315: the remote get is trigger in stable state and the reply received after the rebalance_start command.
    * However, the old owner will receive the request after the state transfer and he no longer has the key.
    */
   public void testScenario_021_11() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s4";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager0.blockBefore(ClusteredGetCommand.class);
      topologyManager0.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      //consistency check. the remote get is triggered
      assertTopologyId(currentTopologyId, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager0.waitForCommandToBlock();

      NewNode joiner = addNode(LatchType.CONSISTENT_HASH_UPDATE);
      topologyManager0.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);

      //wait until the consistent_hash_update arrives in old owner. Also, awaits until the requestor receives the
      //rebalance_start.
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitForTopology(currentTopologyId + 1, cache(0));
      awaitUntilNotInDataContainer(cache(1), key);
      joiner.localTopologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);
      rpcManager0.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager0.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.localTopologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3721: Same as 021_11 but the new owner is already in the stable state.
    */
   public void testScenario_021_21() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s4";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager0.blockBefore(ClusteredGetCommand.class);
      topologyManager0.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      //consistency check. the remote get is triggered
      assertTopologyId(currentTopologyId, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager0.waitForCommandToBlock();

      NewNode joiner = addNode();
      topologyManager0.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);

      //wait until the consistent_hash_update arrives in old owner. Also, awaits until the requestor receives the
      //rebalance_start.
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitForTopology(currentTopologyId + 1, cache(0));
      awaitForTopology(currentTopologyId + 2, cache(2));
      awaitUntilNotInDataContainer(cache(1), key);
      rpcManager0.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager0.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3315: Same as scenario 021_21 but the reply arrives after the requestor is in the stable state.
    */
   public void testScenario_022_11() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s5";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager = replaceRpcManager(cache(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager.blockBefore(ClusteredGetCommand.class);

      //consistency check. trigger the remote get
      assertTopologyId(currentTopologyId, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager.waitForCommandToBlock();

      NewNode joiner = addNode(LatchType.CONSISTENT_HASH_UPDATE);

      //wait until the state transfer ends in old owner and requestor. then let the remote get go.
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitForTopology(currentTopologyId + 2, cache(0));
      awaitUntilNotInDataContainer(cache(1), key);
      joiner.localTopologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 2, cache(0));

      joiner.localTopologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3721: Same as scenario 022_11 but the new owner is also in the stable state when it receives the 2nd request.
    */
   public void testScenario_022_22() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s5";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager0.blockBefore(ClusteredGetCommand.class);

      //consistency check. trigger the remote get
      assertTopologyId(currentTopologyId, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager0.waitForCommandToBlock();

      NewNode joiner = addNode();

      //wait until the state transfer ends in old owner and requestor. then let the remote get go.
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitForTopology(currentTopologyId + 2, cache(0));
      awaitForTopology(currentTopologyId + 2, cache(2));
      awaitUntilNotInDataContainer(cache(1), key);
      rpcManager0.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 2, cache(0));

      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3315: the remote get is done after the REBALANCE_START command.
    * The old owner receives the request after the READ_CH_UPDATE and no longer has the key.
    * The new owner receives the 2nd request before the READ_CH_UPDATE command.
    */
   public void testScenario_121_11() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s6";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager0.blockBefore(ClusteredGetCommand.class);
      topologyManager0.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      NewNode joiner = addNode(LatchType.CONSISTENT_HASH_UPDATE);
      topologyManager0.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);

      //consistency check. trigger the remote get.
      assertTopologyId(currentTopologyId + 1, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager0.waitForCommandToBlock();

      //wait until the consistent_hash_update arrives in old owner
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitUntilNotInDataContainer(cache(1), key);
      joiner.localTopologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);
      rpcManager0.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager0.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.localTopologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3721: Same as scenario 121_11 but the new owner is already in the stable state.
    */
   public void testScenario121_21() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s6";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager0.blockBefore(ClusteredGetCommand.class);
      topologyManager0.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      NewNode joiner = addNode();
      topologyManager0.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);

      //consistency check. trigger the remote get.
      assertTopologyId(currentTopologyId + 1, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager0.waitForCommandToBlock();

      //wait until the consistent_hash_update arrives in old owner
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitForTopology(currentTopologyId + 2, cache(2));
      awaitUntilNotInDataContainer(cache(1), key);
      joiner.localTopologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);
      rpcManager0.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager0.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3315: the remote get is triggered after the rebalance_start command and the reply is received after the
    * consistent_hash_update command. The old owner receives the request after the consistent_hash_update command and no
    * longer has the key.
    */
   public void testScenario7() throws Exception {
      //events:
      //0: remote get target list obtained in topology i+1. reply obtained in topology i+2
      //1: remote get received in topology i+2 (no longer a owner)
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s7";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager.blockBefore(ClusteredGetCommand.class);
      topologyManager.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      NewNode joiner = addNode(LatchType.CONSISTENT_HASH_UPDATE);
      topologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);

      //consistency check. trigger the remote get.
      assertTopologyId(currentTopologyId + 1, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager.waitForCommandToBlock();

      topologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      //wait until the consistent_hash_update arrives in old owner and in the requestor.
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitForTopology(currentTopologyId + 2, cache(0));
      awaitUntilNotInDataContainer(cache(1), key);
      joiner.localTopologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 2, cache(0));

      joiner.localTopologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3315: the remote get is triggered after the rebalance_start command and the reply is received after the
    * consistent_hash_update command.
    * The old owner receives the request after the consistent_hash_update command and no longer has the key.
    * The new owner is already in the stable state when it receives the 2nd request.
    */
   public void testScenario7_1() throws Exception {
      //events:
      //0: remote get target list obtained in topology i+1. reply obtained in topology i+2
      //1: remote get received in topology i+2 (no longer a owner)
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s7";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager0 = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager0 = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager0.blockBefore(ClusteredGetCommand.class);
      topologyManager0.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      NewNode joiner = addNode();
      topologyManager0.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);

      //consistency check. trigger the remote get.
      assertTopologyId(currentTopologyId + 1, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager0.waitForCommandToBlock();

      topologyManager0.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      //wait until the consistent_hash_update arrives in old owner and in the requestor.
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitForTopology(currentTopologyId + 2, cache(0));
      awaitForTopology(currentTopologyId + 2, cache(2));
      awaitUntilNotInDataContainer(cache(1), key);
      rpcManager0.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 2, cache(0));

      joiner.localTopologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.joinerFuture.get();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, configuration());
   }

   private Future<Object> remoteGet(Cache cache, Object key) {
      return fork(new RemoteGetCallable(cache, key));
   }

   private int currentTopologyId(Cache cache) {
      return TestingUtil.extractComponent(cache, StateTransferManager.class).getCacheTopology().getTopologyId();
   }

   private void assertTopologyId(final int expectedTopologyId, final Cache cache) {
      assertEquals(expectedTopologyId, currentTopologyId(cache));
   }

   private void awaitForTopology(final int expectedTopologyId, final Cache cache) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return expectedTopologyId == currentTopologyId(cache);
         }
      });
   }

   private void awaitUntilNotInDataContainer(final Cache cache, final Object key) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !cache.getAdvancedCache().getDataContainer().containsKey(key);
         }
      });
   }

   private NewNode addNode() {
      return addNode(null);
   }

   private NewNode addNode(LatchType block) {
      NewNode newNode = new NewNode();
      EmbeddedCacheManager embeddedCacheManager = addClusterEnabledCacheManager(configuration());
      newNode.localTopologyManager = replaceTopologyManager(embeddedCacheManager);
      if (block != null) {
         newNode.localTopologyManager.startBlocking(block);
      }
      topologyManagerList.add(newNode.localTopologyManager);
      newNode.joinerFuture = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            waitForClusterToForm();
            return null;
         }
      });
      return newNode;
   }

   private void ownerCheckAndInit(Cache<Object, Object> owner, Object key, Object value) {
      assertTrue(address(owner) + " should be the owner of " + key + ".", isFirstOwner(cache(1), key));
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

   private class NewNode {
      Future<Void> joinerFuture;
      BlockingLocalTopologyManager localTopologyManager;
   }

   private class RemoteGetCallable implements Callable<Object> {

      private final Cache cache;
      private final Object key;

      private RemoteGetCallable(Cache cache, Object key) {
         this.cache = cache;
         this.key = key;
      }

      @Override
      public Object call() throws Exception {
         return cache.get(key);
      }
   }
}
