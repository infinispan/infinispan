package org.infinispan.distribution.rehash;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.jgroups.blocks.Response;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertSame;

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
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(3);
      createCluster(builder, 4);
      waitForClusterToForm();
   }

   public void testOldStateResponse() throws Throwable {
      MagicKey k1 = new MagicKey("k1", cache(1));
      MagicKey k2 = new MagicKey("k2", cache(2));
      MagicKey k3 = new MagicKey("k3", cache(3));
      cache(1).put(k1, "v1");
      cache(2).put(k2, "v2");
      cache(3).put(k3, "v3");

      final StateTransferManager stm = cache(0).getAdvancedCache().getComponentRegistry().getStateTransferManager();
      int initialTopologyId = stm.getCacheTopology().getTopologyId();

      RpcManager rm = TestingUtil.extractComponent(cache(0), RpcManager.class);
      ControlledRpcManager crm = new ControlledRpcManager(rm);
      crm.blockBefore(StateRequestCommand.class);
      TestingUtil.replaceComponent(cache(0), RpcManager.class, crm, true);

      cache(3).stop();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            // Wait for the rebalance cache topology to be installed
            return stm.getCacheTopology().getPendingCH() != null;
         }
      });

      // Cache 0 didn't manage to send any StateRequestCommand yet.
      // We'll pretend it got a StateResponseCommand with an older topology id.
      InboundInvocationHandler iih = TestingUtil.extractGlobalComponent(manager(0), InboundInvocationHandler.class);
      StateChunk stateChunk = new StateChunk(0, Collections.<InternalCacheEntry>emptyList(), true);
      StateResponseCommand stateResponseCommand = new StateResponseCommand(CacheContainer.DEFAULT_CACHE_NAME,
            address(3), initialTopologyId, Arrays.asList(stateChunk));
      iih.handle(stateResponseCommand, address(3), null, false);

      crm.stopBlocking();

      TestingUtil.waitForRehashToComplete(cache(0), cache(1), cache(2));

      DataContainer dataContainer = TestingUtil.extractComponent(cache(0), DataContainer.class);
      assertTrue(dataContainer.containsKey(k1));
      assertTrue(dataContainer.containsKey(k2));
      assertTrue(dataContainer.containsKey(k3));
   }

   public void testStateResponseWhileRestartingBrokenTransfers() throws Throwable {
      Address primary = address(1);
      MagicKey k1 = new MagicKey("k1", cache(1));
      cache(0).put(k1, "v1");
      Address backup1 = advancedCache(0).getDistributionManager().locate(k1).get(1);
      Address backup2 = advancedCache(0).getDistributionManager().locate(k1).get(2);
      List<Address> nonOwners = new ArrayList<Address>(advancedCache(0).getRpcManager().getMembers());
      nonOwners.removeAll(Arrays.asList(primary, backup1, backup2));
      Address nonOwner = nonOwners.get(0);
      log.debugf("Starting test with key %s, primary owner %s, backup owners %s and %s, non-owner %s", k1, primary,
            backup1, backup2, nonOwner);

      AdvancedCache nonOwnerCache = manager(nonOwner).getCache().getAdvancedCache();
      final StateTransferManager stm = nonOwnerCache.getComponentRegistry().getStateTransferManager();
      final int initialTopologyId = stm.getCacheTopology().getTopologyId();

      final CheckPoint checkPoint = new CheckPoint();
      replaceInvocationHandler(checkPoint, manager(nonOwner), nonOwner);
      replaceInvocationHandler(checkPoint, manager(primary), nonOwner);
      replaceInvocationHandler(checkPoint, manager(backup1), nonOwner);

      log.debugf("Killing node %s", backup2);
      manager(backup2).getCache().stop();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            // Wait for the rebalance cache topology to be installed
            return stm.getCacheTopology().getTopologyId() == initialTopologyId + 2;
         }
      });

      // Allow the non-owner cache to request transactions from the owners (in any order)
      checkPoint.trigger("OUT_GET_TRANSACTIONS_" + primary);
      checkPoint.trigger("OUT_GET_TRANSACTIONS_" + backup1);
      checkPoint.awaitStrict("IN_GET_TRANSACTIONS_" + primary, 10, SECONDS);
      checkPoint.awaitStrict("IN_GET_TRANSACTIONS_" + backup1, 10, SECONDS);

      // See which cache receives the START_STATE_TRANSFER command first. We'll kill the other.
      String event = checkPoint.peek(5, TimeUnit.SECONDS, "IN_START_STATE_TRANSFER_" + primary,
            "IN_START_STATE_TRANSFER_" + backup1);
      Address liveNode = event.endsWith(primary.toString()) ? primary : backup1;
      Address nodeToKill = liveNode == primary ? backup1 : primary;
      List<Address> keyOwners = nonOwnerCache.getDistributionManager().locate(k1);
      log.debugf("Killing node %s. Key %s is located on %s", nodeToKill, k1, keyOwners);
      log.debugf("Data on node %s: %s", primary, manager(primary).getCache().keySet());
      log.debugf("Data on node %s: %s", backup1, manager(backup1).getCache().keySet());

      // Now that we know which node to kill, allow the START_STATE_TRANSFER command to proceed.
      // The corresponding StateResponseCommand will be blocked on the non-owner
      checkPoint.await("IN_START_STATE_TRANSFER_" + liveNode, 1, SECONDS);
      checkPoint.trigger("OUT_START_STATE_TRANSFER_" + liveNode);

      // Kill cache cacheToStop to force a topology update.
      // The topology update will remove the transfers from cache(nodeToKill).
      manager(nodeToKill).getCache().stop();

      // Now allow cache 0 to process the state from cache(liveNode)
      checkPoint.awaitStrict("IN_RESPONSE_" + liveNode, 10, SECONDS);
      checkPoint.trigger("OUT_RESPONSE_" + liveNode);

      log.debugf("Received segments?");
      Thread.sleep(1000);

      // Wait for cache 0 to request the transactions for the failed segments from cache 1
      checkPoint.awaitStrict("IN_GET_TRANSACTIONS_" + liveNode, 10, SECONDS);
      checkPoint.trigger("OUT_GET_TRANSACTIONS_" + liveNode);

      // ISPN-3120: Now cache 0 should think it finished receiving state. Allow all the commands to proceed.
      checkPoint.awaitStrict("IN_START_STATE_TRANSFER_" + liveNode, 10, SECONDS);
      checkPoint.trigger("OUT_START_STATE_TRANSFER_" + liveNode);

      checkPoint.awaitStrict("IN_RESPONSE_" + liveNode, 10, SECONDS);
      checkPoint.trigger("OUT_RESPONSE_" + liveNode);

      TestingUtil.waitForRehashToComplete(nonOwnerCache, manager(liveNode).getCache());

      log.debugf("Final checkpoint status: %s", checkPoint);
      DataContainer dataContainer = TestingUtil.extractComponent(nonOwnerCache, DataContainer.class);
      assertTrue(dataContainer.containsKey(k1));
   }

   private void replaceInvocationHandler(final CheckPoint checkPoint, final EmbeddedCacheManager manager,
                                         final Address nonOwner)
         throws Throwable {
      final InboundInvocationHandler handler = TestingUtil.extractGlobalComponent(manager,
            InboundInvocationHandler.class);
      InboundInvocationHandler mockHandler = mock(InboundInvocationHandler.class);
      doAnswer(new Answer<Object>() {
               @Override
               public Object answer(InvocationOnMock invocation) throws Throwable {
                  CacheRpcCommand command = (CacheRpcCommand) invocation.getArguments()[0];
                  Address source = (Address) invocation.getArguments()[1];
                  Response response = (Response) invocation.getArguments()[2];
                  boolean preserveOrder = (Boolean) invocation.getArguments()[3];
                  if (command instanceof StateRequestCommand && source.equals(nonOwner)) {
                     StateRequestCommand stateRequestCommand = (StateRequestCommand) command;
                     checkPoint.trigger("IN_" + stateRequestCommand.getType() + '_' + manager.getAddress());
                     checkPoint.awaitStrict("OUT_" + stateRequestCommand.getType() + '_' + manager.getAddress(), 5,
                           SECONDS);
                  } else if (command instanceof StateResponseCommand && manager.getAddress().equals(nonOwner)) {
                     checkPoint.trigger("IN_RESPONSE_" + source);
                     checkPoint.awaitStrict("OUT_RESPONSE_" + source, 5, SECONDS);
                  }
                  handler.handle(command, source, response, preserveOrder);
                  return null;
               }
            }).when(mockHandler).handle(any(CacheRpcCommand.class), any(Address.class), any(Response.class),
            anyBoolean());
      TestingUtil.replaceComponent(manager, InboundInvocationHandler.class, mockHandler, true);
      Transport transport = TestingUtil.extractGlobalComponent(manager, Transport.class);
      CommandAwareRpcDispatcher dispatcher = (CommandAwareRpcDispatcher) TestingUtil.extractField(transport, "dispatcher");
      TestingUtil.replaceField(mockHandler, "inboundInvocationHandler", dispatcher, CommandAwareRpcDispatcher.class);
   }
}
