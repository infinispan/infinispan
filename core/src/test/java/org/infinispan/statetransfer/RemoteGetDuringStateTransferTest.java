package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.infinispan.util.AbstractControlledLocalTopologyManager;
import org.infinispan.util.SingleSegmentConsistentHashFactory;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.infinispan.distribution.DistributionTestHelper.isFirstOwner;
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

   /*
   summary (0: node which requests the remote get, 1: old owner)

   sc | currentTopologyId         | currentTopologyId + 1 (rebalance)     | currentTopologyId + 2 (finish)
   1  | 0:remoteGet+receiveReply  | 1:sendReply                           |
   2  | 0:remoteGet               | 1:sendReply, 0:receiveReply           |
   3  |                           | 0:remoteGet+receiveReply, 1:sendReply |
   4  | 0:remoteGet               | 0:receiveReply                        | 1:sendReply
   5  | 0:remoteGet               |                                       | 0:receiveReply, 1:sendReply
   6  |                           | 0:remoteGet+receiveReply              | 1: sendReply
   7  |                           | 0:remoteGet                           | 0:receiveReply, 1:sendReply
    */

   /**
    * ISPN-3315: In this scenario, a remote get is triggered and the reply received in a stable state. the old owner
    * receives the request after the rebalance_start command.
    */
   public void testScenario1() throws Exception {
      assertClusterSize("Wrong cluster size.", 2);
      final Object key = "key_s1";
      ownerCheckAndInit(cache(1), key, "v");

      final ControlledRpcManager rpcManager = replaceRpcManager(cache(0));
      final ControlledLocalTopologyManager topologyManager = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager.blockBefore(ClusteredGetCommand.class);
      topologyManager.startBlocking(LatchType.REBALANCE);

      //remote get is processed in current topology id.
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager.waitForCommandToBlock();

      Future<Void> joinerFuture = addNode();
      topologyManager.waitToBlock(LatchType.REBALANCE);

      //wait until the rebalance_start arrives in old owner and let the remote get go
      awaitForTopology(currentTopologyId + 1, cache(1));
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is still in currentTopologyId (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId, cache(0));

      topologyManager.stopBlocking(LatchType.REBALANCE);
      joinerFuture.get();
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
      final ControlledLocalTopologyManager topologyManager = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager.blockBefore(ClusteredGetCommand.class);
      topologyManager.startBlocking(LatchType.CONFIRM_REBALANCE);

      //the remote get is triggered in the current topology id.
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager.waitForCommandToBlock();

      Future<Void> joinerFuture = addNode();
      topologyManager.waitToBlock(LatchType.CONFIRM_REBALANCE);

      //wait until the rebalance start arrives in old owner and in the requestor. then let the remote get go.
      awaitForTopology(currentTopologyId + 1, cache(1));
      awaitForTopology(currentTopologyId + 1, cache(0));
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager.stopBlocking(LatchType.CONFIRM_REBALANCE);
      joinerFuture.get();
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
      final ControlledLocalTopologyManager topologyManager = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager.blockBefore(ClusteredGetCommand.class);
      topologyManager.startBlocking(LatchType.CONFIRM_REBALANCE);

      Future<Void> joinerFuture = addNode();
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
      joinerFuture.get();
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
      final ControlledLocalTopologyManager topologyManager = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager.blockBefore(ClusteredGetCommand.class);
      topologyManager.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      //consistency check. the remote get is triggered
      assertTopologyId(currentTopologyId, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager.waitForCommandToBlock();

      Future<Void> joinerFuture = addNode();
      topologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);

      //wait until the consistent_hash_update arrives in old owner. Also, awaits until the requestor receives the
      //rebalance_start.
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitForTopology(currentTopologyId + 1, cache(0));
      awaitUntilNotInDataContainer(cache(1), key);
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joinerFuture.get();
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

      Future<Void> joinerFuture = addNode();

      //wait until the state transfer ends in old owner and requestor. then let the remote get go.
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitForTopology(currentTopologyId + 2, cache(0));
      awaitUntilNotInDataContainer(cache(1), key);
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 2, cache(0));

      joinerFuture.get();
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
      final ControlledLocalTopologyManager topologyManager = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager.blockBefore(ClusteredGetCommand.class);
      topologyManager.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      Future<Void> joinerFuture = addNode();
      topologyManager.waitToBlock(LatchType.CONSISTENT_HASH_UPDATE);

      //consistency check. trigger the remote get.
      assertTopologyId(currentTopologyId + 1, cache(0));
      Future<Object> remoteGetFuture = remoteGet(cache(0), key);
      rpcManager.waitForCommandToBlock();

      //wait until the consistent_hash_update arrives in old owner
      awaitForTopology(currentTopologyId + 2, cache(1));
      awaitUntilNotInDataContainer(cache(1), key);
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 1, cache(0));

      topologyManager.stopBlocking(LatchType.CONSISTENT_HASH_UPDATE);
      joinerFuture.get();
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
      final ControlledLocalTopologyManager topologyManager = replaceTopologyManager(manager(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      rpcManager.blockBefore(ClusteredGetCommand.class);
      topologyManager.startBlocking(LatchType.CONSISTENT_HASH_UPDATE);

      Future<Void> joinerFuture = addNode();
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
      rpcManager.stopBlocking();

      //check the value returned and make sure that the requestor is in the correct topology id (consistency check)
      assertEquals("Wrong value from remote get.", "v", remoteGetFuture.get());
      assertTopologyId(currentTopologyId + 2, cache(0));

      joinerFuture.get();
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

   private Future<Void> addNode() {
      addClusterEnabledCacheManager(configuration());
      return fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            waitForClusterToForm();
            return null;
         }
      });
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

   private ControlledLocalTopologyManager replaceTopologyManager(CacheContainer cacheContainer) {
      LocalTopologyManager manager = TestingUtil.extractGlobalComponent(cacheContainer, LocalTopologyManager.class);
      ControlledLocalTopologyManager controlledLocalTopologyManager = new ControlledLocalTopologyManager(manager);
      TestingUtil.replaceComponent(cacheContainer, LocalTopologyManager.class, controlledLocalTopologyManager, true);
      return controlledLocalTopologyManager;
   }

   private ControlledRpcManager replaceRpcManager(Cache cache) {
      RpcManager manager = TestingUtil.extractComponent(cache, RpcManager.class);
      ControlledRpcManager controlledRpcManager = new ControlledRpcManager(manager);
      TestingUtil.replaceComponent(cache, RpcManager.class, controlledRpcManager, true);
      return controlledRpcManager;
   }

   private static enum LatchType {
      CONSISTENT_HASH_UPDATE,
      CONFIRM_REBALANCE,
      REBALANCE
   }

   @SuppressWarnings("unchecked")
   public static class SingleKeyConsistentHashFactory extends SingleSegmentConsistentHashFactory {

      @Override
      protected List<Address> createOwnersCollection(List<Address> members, int numberOfOwners) {
         assertEquals("Wrong number of owners.", 1, numberOfOwners);
         return Collections.singletonList(members.get(members.size() - 1));
      }
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

   private class ControlledLocalTopologyManager extends AbstractControlledLocalTopologyManager {

      private final Latch blockConfirmRebalance;
      private final Latch blockConsistentHashUpdate;
      private final Latch blockRebalanceStart;

      public ControlledLocalTopologyManager(LocalTopologyManager delegate) {
         super(delegate);
         blockRebalanceStart = new Latch();
         blockConsistentHashUpdate = new Latch();
         blockConfirmRebalance = new Latch();
      }

      public void startBlocking(LatchType type) {
         getLatch(type).enable();
      }

      public void stopBlocking(LatchType type) {
         getLatch(type).disable();
      }

      public void waitToBlock(LatchType type) throws InterruptedException {
         getLatch(type).waitToBlock();
      }

      @Override
      protected final void beforeHandleConsistentHashUpdate(String cacheName, CacheTopology cacheTopology, int viewId) {
         getLatch(LatchType.CONSISTENT_HASH_UPDATE).blockIfNeeded();
      }

      @Override
      protected final void beforeHandleRebalance(String cacheName, CacheTopology cacheTopology, int viewId) {
         getLatch(LatchType.REBALANCE).blockIfNeeded();
      }

      @Override
      protected final void beforeConfirmRebalance(String cacheName, int topologyId, Throwable throwable) {
         getLatch(LatchType.CONFIRM_REBALANCE).blockIfNeeded();
      }

      private Latch getLatch(LatchType type) {
         switch (type) {
            case CONSISTENT_HASH_UPDATE:
               return blockConsistentHashUpdate;
            case CONFIRM_REBALANCE:
               return blockConfirmRebalance;
            case REBALANCE:
               return blockRebalanceStart;
         }
         throw new IllegalStateException("Should never happen!");
      }
   }

   private class Latch {

      private boolean enabled = false;
      private boolean blocked = false;

      public final synchronized void enable() {
         this.enabled = true;
      }

      public final synchronized void disable() {
         this.enabled = false;
         notifyAll();
      }

      public final synchronized void blockIfNeeded() {
         blocked = true;
         notifyAll();
         while (enabled) {
            try {
               wait();
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               return;
            }
         }
      }

      public final synchronized void waitToBlock() throws InterruptedException {
         while (!blocked) {
            wait();
         }
      }

   }
}
