/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.distribution.rehash;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
      MagicKey k1 = new MagicKey("k1", cache(1), cache(2), cache(3));
      cache(0).put(k1, "v1");

      final StateTransferManager stm = cache(0).getAdvancedCache().getComponentRegistry().getStateTransferManager();
      final int initialTopologyId = stm.getCacheTopology().getTopologyId();

      final CheckPoint checkPoint = new CheckPoint();
      replaceInvocationHandler(checkPoint, manager(0), StateResponseCommand.class);
      replaceInvocationHandler(checkPoint, manager(1), StateRequestCommand.class);
      replaceInvocationHandler(checkPoint, manager(2), StateRequestCommand.class);

      log.debugf("Killing node %s", address(3));
      cache(3).stop();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            // Wait for the rebalance cache topology to be installed
            return stm.getCacheTopology().getTopologyId() == initialTopologyId + 2;
         }
      });

      // Allow cache 0 to request transactions from caches 1 and 2 (in any order)
      checkPoint.trigger("OUT_GET_TRANSACTIONS_" + address(1));
      checkPoint.trigger("OUT_GET_TRANSACTIONS_" + address(2));
      checkPoint.awaitStrict("IN_GET_TRANSACTIONS_" + address(1), 10, SECONDS);
      checkPoint.awaitStrict("IN_GET_TRANSACTIONS_" + address(2), 10, SECONDS);

      // See which cache receives the START_STATE_TRANSFER command first. We'll kill the other.
      String event = checkPoint.peek(5, TimeUnit.SECONDS, "IN_START_STATE_TRANSFER_" + address(1),
            "IN_START_STATE_TRANSFER_" + address(2));
      int liveNode = event.endsWith(address(1).toString()) ? 1 : 2;
      int nodeToKill = liveNode == 1 ? 2 : 1;
      List<Address> keyOwners = cache(0).getAdvancedCache().getDistributionManager().locate(k1);
      log.debugf("Killing node %s. Key %s is located on %s", address(nodeToKill), k1, keyOwners);
      log.debugf("Data on node %s: %s", address(1), cache(1).keySet());
      log.debugf("Data on node %s: %s", address(2), cache(2).keySet());

      // Now that we know which node to kill, allow the START_STATE_TRANSFER command to proceed.
      // The corresponding StateResponseCommand will be blocked on cache 0
      checkPoint.await("IN_START_STATE_TRANSFER_" + address(liveNode), 1, SECONDS);
      checkPoint.trigger("OUT_START_STATE_TRANSFER_" + address(liveNode));

      // Kill cache cacheToStop to force a topology update.
      // The topology update will remove the transfers from cache(nodeToKill).
      cache(nodeToKill).stop();

      // Now allow cache 0 to process the state from cache(liveNode)
      checkPoint.awaitStrict("IN_RESPONSE_" + address(liveNode), 10, SECONDS);
      checkPoint.trigger("OUT_RESPONSE_" + address(liveNode));

      log.debugf("Received segments?");
      Thread.sleep(1000);

      // Wait for cache 0 to request the transactions for the failed segments from cache 1
      checkPoint.awaitStrict("IN_GET_TRANSACTIONS_" + address(liveNode), 10, SECONDS);
      checkPoint.trigger("OUT_GET_TRANSACTIONS_" + address(liveNode));

      // ISPN-3120: Now cache 0 should think it finished receiving state. Allow all the commands to proceed.
      checkPoint.awaitStrict("IN_START_STATE_TRANSFER_" + address(liveNode), 10, SECONDS);
      checkPoint.trigger("OUT_START_STATE_TRANSFER_" + address(liveNode));

      checkPoint.awaitStrict("IN_RESPONSE_" + address(liveNode), 10, SECONDS);
      checkPoint.trigger("OUT_RESPONSE_" + address(liveNode));

      TestingUtil.waitForRehashToComplete(cache(0), cache(liveNode));

      log.debugf("Final checkpoint status: %s", checkPoint);
      DataContainer dataContainer = TestingUtil.extractComponent(cache(0), DataContainer.class);
      assertTrue(dataContainer.containsKey(k1));
   }

   private void replaceInvocationHandler(final CheckPoint checkPoint, final EmbeddedCacheManager manager,
                                         Class<? extends CacheRpcCommand> commandClass)
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
                  if (command instanceof StateRequestCommand && source.equals(address(0))) {
                     StateRequestCommand stateRequestCommand = (StateRequestCommand) command;
                     checkPoint.trigger("IN_" + stateRequestCommand.getType() + '_' + manager.getAddress());
                     checkPoint.awaitStrict("OUT_" + stateRequestCommand.getType() + '_' + manager.getAddress(), 5,
                           SECONDS);
                  } else if (command instanceof StateResponseCommand && manager.getAddress().equals(address(0))) {
                     checkPoint.trigger("IN_RESPONSE_" + source);
                     checkPoint.awaitStrict("OUT_RESPONSE_" + source, 5, SECONDS);
                  }
                  handler.handle(command, source, response, preserveOrder);
                  return null;
               }
            }).when(mockHandler).handle(any(commandClass), any(Address.class), any(Response.class), anyBoolean());
      TestingUtil.replaceComponent(manager, InboundInvocationHandler.class, mockHandler, true);
      Transport transport = TestingUtil.extractGlobalComponent(manager, Transport.class);
      CommandAwareRpcDispatcher dispatcher = (CommandAwareRpcDispatcher) TestingUtil.extractField(transport, "dispatcher");
      TestingUtil.replaceField(mockHandler, "inboundInvocationHandler", dispatcher, CommandAwareRpcDispatcher.class);
   }
}
