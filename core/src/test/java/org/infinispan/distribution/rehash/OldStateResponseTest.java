/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.InboundInvocationHandlerImpl;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.testng.Assert.assertTrue;

/**
 * Start two rebalance operations by stopping two members of a cluster in sequence.
 * Test that a delayed StateResponseCommand from an already stopped member doesn't break state transfer.
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.OldStateResponseTest")
public class OldStateResponseTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      createCluster(builder, 3);
      waitForClusterToForm();
   }

   public void testOldStateResponse() throws Throwable {
      MagicKey k1 = new MagicKey("k1", cache(1));
      MagicKey k2 = new MagicKey("k2", cache(2));
      cache(1).put(k1, "v1");
      cache(2).put(k2, "v2");

      final StateTransferManager stm = cache(0).getAdvancedCache().getComponentRegistry().getStateTransferManager();
      int initialTopologyId = stm.getCacheTopology().getTopologyId();

      RpcManager rm = TestingUtil.extractComponent(cache(0), RpcManager.class);
      ControlledRpcManager crm = new ControlledRpcManager(rm);
      crm.blockBefore(StateRequestCommand.class);
      TestingUtil.replaceComponent(cache(0), RpcManager.class, crm, true);

      cache(2).stop();

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
            address(2), initialTopologyId, Arrays.asList(stateChunk));
      iih.handle(stateResponseCommand, address(2), null, false);

      crm.stopBlocking();

      TestingUtil.waitForRehashToComplete(cache(0), cache(1));

      DataContainer dataContainer = TestingUtil.extractComponent(cache(0), DataContainer.class);
      assertTrue(dataContainer.containsKey(k1));
      assertTrue(dataContainer.containsKey(k2));
   }

   private void awaitStrict(CountDownLatch latch, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
      if (!latch.await(timeout, unit))
         throw new TimeoutException();
   }
}
