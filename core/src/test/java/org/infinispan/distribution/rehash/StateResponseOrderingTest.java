package org.infinispan.distribution.rehash;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.waitForNoRebalance;
import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnOutboundRpc;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.statetransfer.StateTransferGetTransactionsCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateCacheConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.MagicKey;
import org.infinispan.reactive.publisher.impl.commands.batch.PublisherResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.RequestRepository;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.CommandMatcher;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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
      consistentHashFactory = new ControlledConsistentHashFactory.Default(new int[][]{{1, 2, 3}, {1, 2, 3}});
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(3);
      builder.clustering().hash().numSegments(2);
      builder.addModule(PrivateCacheConfigurationBuilder.class).consistentHashFactory(consistentHashFactory);
      createCluster(ControlledConsistentHashFactory.SCI.INSTANCE, builder, 4);
      waitForClusterToForm();
   }

   private RequestRepository spyRequestRepository(Cache<Object, Object> cache) {
      // This is assumed to be a JGroupsTransport
      Transport transport = extractComponent(cache, Transport.class);
      return Mocks.replaceFieldWithSpy(transport, "requests");
   }

   public void testStateResponseWhileRestartingBrokenTransfers() throws Throwable {
      // The initial topology is different from the other method's
      consistentHashFactory.setOwnerIndexes(new int[][]{{1, 2, 3}, {2, 1, 3}});
      consistentHashFactory.triggerRebalance(cache(0));
      // waitForStableTopology doesn't work here, since the cache looks already "balanced"
      // So we wait for the primary owner of segment 1 to change
      eventuallyEquals(address(2), () -> advancedCache(0).getDistributionManager().getCacheTopology().getReadConsistentHash().locatePrimaryOwnerForSegment(1));

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

      DistributionManager dm0 = advancedCache(0).getDistributionManager();
      StateTransferManager stm0 = TestingUtil.extractComponentRegistry(cache(0)).getStateTransferManager();

      MagicKey k1 = new MagicKey("k1", cache(1));
      assertEquals(Arrays.asList(address(1), address(2), address(3)),
                   dm0.getCacheTopology().getDistribution(k1).readOwners());
      cache(0).put(k1, "v1");
      MagicKey k2 = new MagicKey("k2", cache(2));
      assertEquals(Arrays.asList(address(2), address(1), address(3)),
                   dm0.getCacheTopology().getDistribution(k2).readOwners());
      cache(0).put(k2, "v2");

      // Setup Spy and Interceptors after data population to avoid interference with puts
      final RequestRepository spyRequests = spyRequestRepository(cache(0));
      final AtomicReference<Address> firstResponseSender = new AtomicReference<>();
      final AtomicInteger responseCount = new AtomicInteger(0);

      doAnswer(invocation -> {
         Response response = invocation.getArgument(2);
         if (response instanceof ValidResponse) {
            Object responseValue = ((ValidResponse) response).getResponseValue();
            if (responseValue instanceof PublisherResponse) {
               if (responseCount.getAndIncrement() == 0) {
                  Address sender = invocation.getArgument(1);
                  firstResponseSender.set(sender);
                  log.debugf("Blocking first state response from %s", sender);

                  sequencer.advance("st:block_first_state_response");
                  sequencer.advance("st:resume_first_state_response");
                  log.debugf("Resuming first state response from %s", sender);
                  Object result = invocation.callRealMethod();
                  sequencer.advance("st:after_first_state_response");
                  return result;
               }
            }
         }
         return invocation.callRealMethod();
      }).when(spyRequests).addResponse(anyLong(), any(), any());

      CommandMatcher secondStateRequestMatcher = new CommandMatcher() {
         private final AtomicInteger counter = new AtomicInteger();

         @Override
         public boolean accept(ReplicableCommand command) {
            if (command instanceof StateTransferGetTransactionsCommand) {
               // Commands 0 and 1 are sent during the first rebalance
               // Command 2 is the first sent after the node is killed
               int c = counter.getAndIncrement();
               log.debugf("Intercepted StateTransferGetTransactionsCommand #%d: %s", c, command);
               if (c == 2)
                  return true;
            }
            return false;
         }
      };
      advanceOnOutboundRpc(sequencer, cache(0), secondStateRequestMatcher)
            .before("st:block_second_state_request", "st:resume_second_state_request");

      // Start the rebalance
      consistentHashFactory.setOwnerIndexes(new int[][]{{0, 1, 2}, {0, 2, 1}});
      consistentHashFactory.triggerRebalance(cache(0));

      // Wait for cache0 to receive the state response
      sequencer.enter("st:kill_node");

      assertNotNull(dm0.getCacheTopology().getPendingCH());

      // No need to update the owner indexes, the CH factory only knows about the cache members
      int nodeToKeep = managerIndex(firstResponseSender.get());
      int nodeToKill = nodeToKeep == 1 ? 2 : 1;
      log.debugf("Blocked state response from %s, killing %s", firstResponseSender.get(), manager(nodeToKill));
      cache(nodeToKill).stop();
      eventuallyEquals(3, () -> dm0.getCacheTopology().getMembers().size());

      sequencer.exit("st:kill_node");

      sequencer.enter("st:check_incomplete");
      assertTrue(stm0.isStateTransferInProgress());
      sequencer.exit("st:check_incomplete");

      // Only the 3 live caches are in the collection, wait for the rehash to end
      waitForNoRebalance(cache(0), cache(nodeToKeep), cache(3));

      assertTrue(dm0.getCacheTopology().isReadOwner(k1));
      assertTrue(dm0.getCacheTopology().isReadOwner(k2));
      assertEquals("v1", cache(0).get(k1));
      assertEquals("v2", cache(0).get(k2));
   }
}
