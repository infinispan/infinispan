/*
 * JBoss, Home of Professional Open Source
 *  Copyright 2013 Red Hat Inc. and/or its affiliates and other
 *  contributors as indicated by the @author tags. All rights reserved
 *  See the copyright.txt in the distribution for a full listing of
 *  individual contributors.
 *
 *  This is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as
 *  published by the Free Software Foundation; either version 2.1 of
 *  the License, or (at your option) any later version.
 *
 *  This software is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this software; if not, write to the Free
 *  Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 *  02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.statetransfer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.InfinispanCollections;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;

/**
 * tests scenario for ISPN-2574
 *
 * - create nodes A, B - start node C - starts state transfer from B to C
 * - abruptly kill B before it is able to send StateResponse to C
 * - C resends the request to A
 * - finally cluster A, C is formed where all entries are properly backed up on both nodes
 *
 * @author Michal Linhard
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.StateTransferRestartTest")
@CleanupAfterMethod
public class StateTransferRestartTest extends MultipleCacheManagersTest {

   private ConfigurationBuilder cfgBuilder;
   private GlobalConfigurationBuilder gcfgBuilder;

   private class MockTransport extends JGroupsTransport {
      volatile Callable<Void> callOnStateResponseCommand;

      @Override
      public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout,
                                                   boolean usePriorityQueue, ResponseFilter responseFilter) throws Exception {
         if (callOnStateResponseCommand != null && rpcCommand.getClass() == StateResponseCommand.class) {
            log.trace("Ignoring StateResponseCommand");
            try {
               callOnStateResponseCommand.call();
            } catch (Exception e) {
               log.error("Error in callOnStateResponseCommand", e);
            }
            return InfinispanCollections.emptyMap();
         }
         return super.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter);
      }
   }

   private MockTransport mockTransport = new MockTransport();

   private void waitForStateTransfer(Cache... caches) throws InterruptedException {
      StateTransferManager[] stm = new StateTransferManager[caches.length];
      for (int i = 0; i < stm.length; i++) {
         stm[i] = TestingUtil.extractComponent(caches[i], StateTransferManager.class);
      }
      while (true) {
         boolean inProgress = false;
         for (StateTransferManager aStm : stm) {
            if (aStm.isStateTransferInProgress()) {
               inProgress = true;
               break;
            }
         }
         if (!inProgress) {
            break;
         }
         wait(100);
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      cfgBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cfgBuilder.transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      cfgBuilder.clustering().hash().numOwners(2);
      cfgBuilder.clustering().stateTransfer().fetchInMemoryState(true);
      cfgBuilder.clustering().stateTransfer().timeout(20000);

      gcfgBuilder = new GlobalConfigurationBuilder();
      gcfgBuilder.transport().transport(mockTransport);
   }

   public void testStateTransferRestart() throws Throwable {
      final int numKeys = 100;

      addClusterEnabledCacheManager(cfgBuilder, new TransportFlags().withFD(true));
      addClusterEnabledCacheManager(gcfgBuilder, cfgBuilder, new TransportFlags().withFD(true));
      log.info("waiting for cluster { c0, c1 }");
      waitForClusterToForm();

      log.info("putting in data");
      final Cache<Object, Object> c0 = cache(0);
      final Cache<Object, Object> c1 = cache(1);
      for (int k = 0; k < numKeys; k++) {
         c0.put(k, k);
      }
      waitForStateTransfer(c0, c1);

      assertEquals(numKeys, c0.entrySet().size());
      assertEquals(numKeys, c1.entrySet().size());

      mockTransport.callOnStateResponseCommand = new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            fork(new Callable<Void>() {
               @Override
               public Void call() throws Exception {
                  log.info("KILLING the c1 cache");
                  try {
                     DISCARD d3 = TestingUtil.getDiscardForCache(c1);
                     d3.setDiscardAll(true);
                     d3.setExcludeItself(true);
                     TestingUtil.killCacheManagers(manager(c1));
                  } catch (Exception e) {
                     log.info("there was some exception while killing cache");
                  }
                  return null;
               }
            });
            try {
               // sleep and wait to be killed
               Thread.sleep(25000);
            } catch (InterruptedException e) {
               log.info("Interrupted as expected.");
               Thread.currentThread().interrupt();
            }
            return null;
         }
      };

      log.info("adding cache c2");
      addClusterEnabledCacheManager(cfgBuilder, new TransportFlags().withFD(true));
      log.info("get c2");
      final Cache<Object, Object> c2 = cache(2);

      log.info("waiting for cluster { c0, c2 }");
      TestingUtil.blockUntilViewsChanged(10000, 2, c0, c2);

      log.infof("c0 entrySet size before : %d", c0.entrySet().size());
      log.infof("c2 entrySet size before : %d", c2.entrySet().size());

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return c0.entrySet().size() == numKeys && c2.entrySet().size() == numKeys;
         }
      });

      log.info("Ending the test");
   }
}
