package org.infinispan.distribution.rehash;

import static org.infinispan.test.TestingUtil.waitForStableTopology;
import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnInboundRpc;
import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnOutboundRpc;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchCommand;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.CommandMatcher;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ByteString;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

/**
 * Start two rebalance operations by stopping two members of a cluster in sequence.
 * Test that a delayed StateResponseCommand doesn't break state transfer.
 * See https://issues.jboss.org/browse/ISPN-3120
 *
 * @author Dan Berindei
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "distribution.rehash.StateResponseOrderingTest")
public class StateResponseOrderingTest extends MultipleCacheManagersTest {

   private ControlledConsistentHashFactory consistentHashFactory;

   @Override
   protected void createCacheManagers() throws Throwable {
      consistentHashFactory = new ControlledConsistentHashFactory(new int[]{1, 2, 3}, new int[]{1, 2, 3});
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(3);
      builder.clustering().hash().numSegments(2).consistentHashFactory(consistentHashFactory);
      createCluster(builder, 4);
      waitForClusterToForm();
   }

   public void testSimulatedOldStateResponse() throws Throwable {
      // Initial owners for both segments are cache 1 and cache 2
      // Start a rebalance, with cache 0 becoming an owner of both CH segments
      // Block the first StateRequestCommand on cache 0
      // While state transfer is blocked, simulate an old state response command on cache 0
      // Check that the old command is ignored and state transfer completes successfully
      StateSequencer sequencer = new StateSequencer();
      sequencer.logicalThread("st", "st:block_state_request", "st:simulate_old_response", "st:resume_state_request");

      cache(1).put("k1", "v1");
      cache(2).put("k2", "v2");
      cache(3).put("k3", "v3");

      final StateTransferManager stm0 = advancedCache(0).getComponentRegistry().getStateTransferManager();
      final int initialTopologyId = stm0.getCacheTopology().getTopologyId();

      assertEquals(Arrays.asList(address(1), address(2), address(3)), stm0.getCacheTopology().getCurrentCH().locateOwners("k1"));
      assertNull(stm0.getCacheTopology().getPendingCH());

      // Block when cache 0 sends the first state request to cache 1
      CommandMatcher segmentRequestMatcher = new CommandMatcher() {
         @Override
         public boolean accept(ReplicableCommand command) {
            if (!(command instanceof StateRequestCommand))
               return false;
            StateRequestCommand stateRequestCommand = (StateRequestCommand) command;
            if (stateRequestCommand.getType() != StateRequestCommand.Type.START_STATE_TRANSFER)
               return false;
            return stateRequestCommand.getTopologyId() == initialTopologyId + 1;
         }
      };
      advanceOnOutboundRpc(sequencer, cache(0), segmentRequestMatcher)
            .before("st:block_state_request", "st:resume_state_request");

      // Cache 0 will become an owner and will request state from cache 1
      consistentHashFactory.setOwnerIndexes(new int[]{0, 1, 2}, new int[]{0, 1, 2});
      consistentHashFactory.triggerRebalance(cache(0));

      sequencer.enter("st:simulate_old_response");

      assertNotNull(stm0.getCacheTopology().getPendingCH());
      assertEquals(Arrays.asList(address(0), address(1), address(2)), stm0.getCacheTopology().getPendingCH().locateOwners("k1"));

      // Cache 0 didn't manage to request any segments yet, but it has registered all the inbound transfer tasks.
      // We'll pretend it got a StateResponseCommand with an older topology id.
      PerCacheInboundInvocationHandler handler = TestingUtil.extractComponent(cache(0), PerCacheInboundInvocationHandler.class);
      StateChunk stateChunk0 = new StateChunk(0, Arrays.<InternalCacheEntry>asList(new ImmortalCacheEntry("k0", "v0")), true);
      StateChunk stateChunk1 = new StateChunk(1, Arrays.<InternalCacheEntry>asList(new ImmortalCacheEntry("k0", "v0")), true);
      StateResponseCommand stateResponseCommand = new StateResponseCommand(ByteString.fromString(CacheContainer.DEFAULT_CACHE_NAME),
            address(1), initialTopologyId, Arrays.asList(stateChunk0, stateChunk1));
      // Call with preserveOrder = true to force the execution in the same thread
      stateResponseCommand.setOrigin(address(3));
      stateResponseCommand.init(TestingUtil.extractComponent(cache(0), StateConsumer.class));
      handler.handle(stateResponseCommand, new Reply() {
         @Override
         public void reply(Object returnValue) {
            //no-op
         }
      }, DeliverOrder.PER_SENDER);

      sequencer.exit("st:simulate_old_response");

      waitForStableTopology(cache(0), cache(1), cache(2), cache(3));

      // Check that state wasn't lost
      assertTrue(stm0.getCacheTopology().getReadConsistentHash().isKeyLocalToNode(address(0), "k1"));
      assertTrue(stm0.getCacheTopology().getReadConsistentHash().isKeyLocalToNode(address(0), "k2"));
      assertTrue(stm0.getCacheTopology().getReadConsistentHash().isKeyLocalToNode(address(0), "k3"));
      assertEquals("v1", cache(0).get("k1"));
      assertEquals("v2", cache(0).get("k2"));
      assertEquals("v3", cache(0).get("k3"));
      // Check that the old state response was ignored
      assertNull(cache(0).get("k0"));
   }

   public void testStateResponseWhileRestartingBrokenTransfers() throws Throwable {
      // The initial topology is different from the other method's
      consistentHashFactory.setOwnerIndexes(new int[]{1, 2, 3}, new int[]{2, 1, 3});
      consistentHashFactory.triggerRebalance(cache(0));
      // waitForStableTopology doesn't work here, since the cache looks already "balanced"
      // So we wait for the primary owner of segment 1 to change
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return advancedCache(0).getDistributionManager().getReadConsistentHash().locatePrimaryOwnerForSegment(1).equals(address(2));
         }
      });

      // See https://issues.jboss.org/browse/ISPN-3120?focusedCommentId=12777231
      // Start with segment 0 owned by [cache1, cache2, cache3], and segment 1 owned by [cache2, cache1, cache3]
      // Trigger a rebalance with cache0 becoming an owner for both segments
      // Wait for either cache1 or cache2 to send a StateResponseCommand
      // Block the state response on cache0
      // Kill the node that didn't receive the request
      // Block new state requests from cache0 so that the killed node's segment doesn't have a transfer task
      // Unblock the first state response
      // Check that the StateResponseCommand hasn't marked state transfer as completed
      // Unblock the new state request
      // Wait for the state transfer to end and check that state hasn't been lost
      StateSequencer sequencer = new StateSequencer();
      sequencer.logicalThread("st", "st:block_first_state_response", "st:kill_node", "st:block_second_state_request",
            "st:resume_first_state_response", "st:after_first_state_response", "st:check_incomplete",
            "st:resume_second_state_request");

      final AtomicReference<Address> firstResponseSender = new AtomicReference<>();
      CommandMatcher firstStateResponseMatcher = new CommandMatcher() {
         CommandMatcher realMatcher = matchCommand(StateResponseCommand.class).matchCount(0).build();

         public boolean accept(ReplicableCommand command) {
            if (!realMatcher.accept(command))
               return false;
            firstResponseSender.set(((StateResponseCommand) command).getOrigin());
            return true;
         }
      };
      advanceOnInboundRpc(sequencer, cache(0), firstStateResponseMatcher)
            .before("st:block_first_state_response", "st:resume_first_state_response")
            .after("st:after_first_state_response");

      CommandMatcher secondStateRequestMatcher = new CommandMatcher() {
         private final AtomicInteger counter = new AtomicInteger();

         @Override
         public boolean accept(ReplicableCommand command) {
            if (command instanceof StateRequestCommand) {
               StateRequestCommand stateRequestCommand = (StateRequestCommand) command;
               if (stateRequestCommand.getType() == StateRequestCommand.Type.GET_TRANSACTIONS) {
                  // Commands 0 and 1 are sent during the first rebalance
                  // Command 2 is the first sent after the node is killed
                  if (counter.getAndIncrement() == 2)
                     return true;
                  log.debugf("Not blocking command %s", command);
               }
            }
            return false;
         }
      };
      advanceOnOutboundRpc(sequencer, cache(0), secondStateRequestMatcher)
            .before("st:block_second_state_request", "st:resume_second_state_request");

      final StateTransferManager stm0 = advancedCache(0).getComponentRegistry().getStateTransferManager();

      MagicKey k1 = new MagicKey("k1", cache(1));
      assertEquals(Arrays.asList(address(1), address(2), address(3)), stm0.getCacheTopology().getCurrentCH().locateOwners(k1));
      cache(0).put(k1, "v1");
      MagicKey k2 = new MagicKey("k2", cache(2));
      assertEquals(Arrays.asList(address(2), address(1), address(3)), stm0.getCacheTopology().getCurrentCH().locateOwners(k2));
      cache(0).put(k2, "v2");

      // Start the rebalance
      consistentHashFactory.setOwnerIndexes(new int[]{0, 1, 2}, new int[]{0, 2, 1});
      consistentHashFactory.triggerRebalance(cache(0));

      // Wait for cache0 to receive the state response
      sequencer.enter("st:kill_node");

      assertNotNull(stm0.getCacheTopology().getPendingCH());

      // No need to update the owner indexes, the CH factory only knows about the cache members
      int nodeToKeep = managerIndex(firstResponseSender.get());
      int nodeToKill = nodeToKeep == 1 ? 2 : 1;
      log.debugf("Blocked state response from %s, killing %s", firstResponseSender.get(), manager(nodeToKill));
      cache(nodeToKill).stop();
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return stm0.getCacheTopology().getMembers().size() == 3;
         }
      });

      sequencer.exit("st:kill_node");

      sequencer.enter("st:check_incomplete");
      assertTrue(stm0.isStateTransferInProgress());
      sequencer.exit("st:check_incomplete");

      // Only the 3 live caches are in the collection, wait for the rehash to end
      TestingUtil.waitForStableTopology(cache(0), cache(nodeToKeep), cache(3));

      assertTrue(stm0.getCacheTopology().getReadConsistentHash().isKeyLocalToNode(address(0), k1));
      assertTrue(stm0.getCacheTopology().getReadConsistentHash().isKeyLocalToNode(address(0), k2));
      assertEquals("v1", cache(0).get(k1));
      assertEquals("v2", cache(0).get(k2));
   }
}
