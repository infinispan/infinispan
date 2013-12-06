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
import org.infinispan.util.SingleSegmentConsistentHashFactory;
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
@Test(groups = "functional", testName = "statetransfer.RemoteGetDuringStateTransferTest")
@CleanupAfterMethod
public class RemoteGetDuringStateTransferTest extends MultipleCacheManagersTest {

   private final List<BlockingLocalTopologyManager> topologyManagerList =
         Collections.synchronizedList(new ArrayList<BlockingLocalTopologyManager>(4));
   private final List<ControlledRpcManager> rpcManagerList =
         Collections.synchronizedList(new ArrayList<ControlledRpcManager>(4));

   /*
   summary (0: node which requests the remote get, 1: old owner, 2 is the new owner)

   sc | currentTopologyId         | currentTopologyId + 1 (rebalance)     | currentTopologyId + 2 (finish)
   1  | 0:remoteGet+receiveReply  | 1:sendReply                           |
   2  | 0:remoteGet               | 1:sendReply, 0:receiveReply           |
   3  |                           | 0:remoteGet+receiveReply, 1:sendReply |
   4  | 0:remoteGet               | 0:receiveReply, 2:sendReply           | 1:sendReply
   4.1| 0:remoteGet               | 0:receiveReply                        | 1:sendReply, 2:sendReply
   5  | 0:remoteGet               | 2:sendReply                           | 0:receiveReply, 1:sendReply
   5.1| 0:remoteGet               |                                       | 0:receiveReply, 1:sendReply, 2:sendReply
   6  |                           | 0:remoteGet+receiveReply, 2:sendReply | 1:sendReply
   6.1|                           | 0:remoteGet+receiveReply              | 1:sendReply, 2:sendReply
   7  |                           | 0:remoteGet, 2: sendReply             | 0:receiveReply, 1:sendReply
   7.1|                           | 0:remoteGet                           | 0:receiveReply, 1:sendReply, 2::sendReply
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
   public void testScenario1() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s1";
      ownerCheckAndInit(cache(1), key, "v");

      final ControlledRpcManager rpcManager = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager.blockBefore(ClusteredGetCommand.class);
      topologyManager.startBlocking(LatchType.REBALANCE);

      //remote get is processed in current topology id.
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager.waitForCommandToBlock();

      NewNode joiner = addNode();
      topologyManager.waitToBlock(LatchType.REBALANCE);

      //wait until the rebalance_start arrives in old owner and let the remote get go
      awaitForTopology(currentTopologyId + 1, cache(1));
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is still in currentTopologyId (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId, cache(0));

      topologyManager.stopBlocking(LatchType.REBALANCE);
      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3315: similar to scenario 1, the remote get is triggered in stable state but reply is received after the
    * rebalance_start command. As in scenario 1, the owner receives the request after the rebalance_start command.
    */
   public void testScenario2() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s2";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager.blockBefore(ClusteredGetCommand.class);
      topologyManager.startBlocking(LatchType.CONFIRM_REBALANCE);

      //the remote get is triggered in the current topology id.
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager.waitForCommandToBlock();

      NewNode joiner = addNode();
      topologyManager.waitToBlock(LatchType.CONFIRM_REBALANCE);

      //wait until the rebalance start arrives in old owner and in the requestor. then let the remote get go.
      awaitForTopology(currentTopologyId + 1, cache(1));
      awaitForTopology(currentTopologyId + 1, cache(0));
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager.stopBlocking(LatchType.CONFIRM_REBALANCE);
      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3315: the remote get is triggered and the reply received after the rebalance_start command. As in previous
    * scenario, the old owner receives the request after the rebalance_start command.
    */
   public void testScenario3() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s3";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager.blockBefore(ClusteredGetCommand.class);
      topologyManager.startBlocking(LatchType.CONFIRM_REBALANCE);

      NewNode joiner = addNode();
      topologyManager.waitToBlock(LatchType.CONFIRM_REBALANCE);

      //consistency check
      awaitForTopology(currentTopologyId + 1, cache(0));

      //the remote get is triggered after the rebalance_start and before the confirm_rebalance.
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager.waitForCommandToBlock();

      //wait until the rebalance_start arrives in old owner
      awaitForTopology(currentTopologyId + 1, cache(1));
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager.stopBlocking(LatchType.CONFIRM_REBALANCE);
      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3315: the remote get is trigger in stable state and the reply received after the rebalance_start command.
    * However, the old owner will receive the request after the state transfer and he no longer has the key.
    */
   public void testScenario4() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s4";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager.blockBefore(ClusteredGetCommand.class);
      topologyManager.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      //consistency check. the remote get is triggered
      assertTopologyId(currentTopologyId, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager.waitForCommandToBlock();

      NewNode joiner = addNode(LatchType.CONSISTENT_HASH_UPDATE);
      topologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);

      //wait until the consistent_hash_update arrives in old owner. Also, awaits until the requestor receives the
      //rebalance_start.
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitForTopology(currentTopologyId + 1, cache(0));
      awaitUntilNotInDataContainer(cache(1), key);
      joiner.localTopologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.localTopologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3721: Same as scenario4 but the new owner is already in the stable state.
    */
   public void testScenario4_1() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s4";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager.blockBefore(ClusteredGetCommand.class);
      topologyManager.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      //consistency check. the remote get is triggered
      assertTopologyId(currentTopologyId, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager.waitForCommandToBlock();

      NewNode joiner = addNode();
      topologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);

      //wait until the consistent_hash_update arrives in old owner. Also, awaits until the requestor receives the
      //rebalance_start.
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitForTopology(currentTopologyId + 1, cache(0));
      awaitForTopology(currentTopologyId + 2, cache(2));
      awaitUntilNotInDataContainer(cache(1), key);
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3315: similar to scenario 4, but this time the reply arrives after the state transfer.
    */
   public void testScenario5() throws Exception {
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
    * ISPN-3721: Same as scenario5 but the new owner is already in the stable state.
    */
   public void testScenario5_1() throws Exception {
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

      NewNode joiner = addNode();

      //wait until the state transfer ends in old owner and requestor. then let the remote get go.
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitForTopology(currentTopologyId + 2, cache(0));
      awaitForTopology(currentTopologyId + 2, cache(2));
      awaitUntilNotInDataContainer(cache(1), key);
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 2, cache(0));

      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3315: the remote get and the reply are done after the rebalance_start command. The old owner receives the
    * request after the consistent_hash_update and no longer has the key
    */
   public void testScenario6() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s6";
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

      //wait until the consistent_hash_update arrives in old owner
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitUntilNotInDataContainer(cache(1), key);
      joiner.localTopologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.localTopologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joiner.joinerFuture.get();
   }

   /**
    * ISPN-3721: Same as scenario6 but the new owner is already in the stable state.
    */
   public void testScenario6_1() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s6";
      ownerCheckAndInit(cache(1), key, "v");
      final ControlledRpcManager rpcManager = replaceRpcManager(cache(0));
      final BlockingLocalTopologyManager topologyManager = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager.blockBefore(ClusteredGetCommand.class);
      topologyManager.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      NewNode joiner = addNode();
      topologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);

      //consistency check. trigger the remote get.
      assertTopologyId(currentTopologyId + 1, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager.waitForCommandToBlock();

      //wait until the consistent_hash_update arrives in old owner
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitForTopology(currentTopologyId + 2, cache(2));
      awaitUntilNotInDataContainer(cache(1), key);
      joiner.localTopologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
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
    * ISPN-3721: Same as scenario7 but the new owner is already in the stable state.
    */
   public void testScenario7_1() throws Exception {
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

      NewNode joiner = addNode();
      topologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);

      //consistency check. trigger the remote get.
      assertTopologyId(currentTopologyId + 1, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager.waitForCommandToBlock();

      topologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      //wait until the consistent_hash_update arrives in old owner and in the requestor.
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitForTopology(currentTopologyId + 2, cache(0));
      awaitForTopology(currentTopologyId + 2, cache(2));
      awaitUntilNotInDataContainer(cache(1), key);
      rpcManager.stopBlocking();

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
   public static class SingleKeyConsistentHashFactory extends SingleSegmentConsistentHashFactory {

      @Override
      protected List<Address> createOwnersCollection(List<Address> members, int numberOfOwners) {
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
