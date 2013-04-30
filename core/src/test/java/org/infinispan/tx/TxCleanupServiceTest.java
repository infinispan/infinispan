/*
 * JBoss, Home of Professional Open Source
 *  Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.tx;


import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.util.mocks.ControlledCommandFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

/**
 * Test for https://issues.jboss.org/browse/ISPN-2383.
 *
 * A single execution of this test (or multiple for that reason)
 * might not verify the tested condition( that is  a transaction is not lost when the main owner of the single key it
 * touched has changed as a result of a node joining). That is because the key is generated before the new node to join
 * so we have no guarantees that the key will map to the new joiner (this is what we want to test). Running the test
 * several times significantly increases the chances for this happen.
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "tx.TxCleanupServiceTest", invocationCount = 10)
public class TxCleanupServiceTest extends MultipleCacheManagersTest {
   private static final int TX_COUNT = 1;
   private ConfigurationBuilder dcc;

   @Override
   protected void createCacheManagers() throws Throwable {
      dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      dcc.clustering().hash().numOwners(1).stateTransfer().fetchInMemoryState(true);
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   public void testTransactionStateNotLost() throws Throwable {
      final ControlledCommandFactory ccf = ControlledCommandFactory.registerControlledCommandFactory(cache(1), CommitCommand.class);
      ccf.gate.close();

      final Map<Object, DummyTransaction> keys2Tx = new HashMap<Object, DummyTransaction>(TX_COUNT);

      int viewId = advancedCache(0).getRpcManager().getTransport().getViewId();

      log.tracef("ViewId before %s", viewId);

      //fork it into another thread as this is going to block in commit
      fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            for (int i = 0; i < TX_COUNT; i++) {
               Object k = getKeyForCache(1);
               tm(0).begin();
               cache(0).put(k, k);
               DummyTransaction transaction = ((DummyTransactionManager) tm(0)).getTransaction();
               keys2Tx.put(k, transaction);
               tm(0).commit();
            }
            return null;
         }
      });

      //now wait for all the commits to block
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return ccf.blockTypeCommandsReceived.get() == TX_COUNT;
         }
      });

      log.tracef("Viewid middle %s", viewId);


      //now add a one new member
      addClusterEnabledCacheManager(dcc);
      waitForClusterToForm();


      viewId = advancedCache(0).getRpcManager().getTransport().getViewId();

      log.tracef("Viewid after before %s", viewId);


      final Map<Object, DummyTransaction> migratedTx = new HashMap<Object, DummyTransaction>(TX_COUNT);
      for (Object key : keys2Tx.keySet()) {
         if (keyMapsToNode(key, 2)) {
            migratedTx.put(key, keys2Tx.get(key));
         }
      }

      log.tracef("Number of migrated tx is %s", migratedTx.size());
      if (migratedTx.size() == 0) return;

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return TestingUtil.getTransactionTable(cache(2)).getRemoteTxCount() == migratedTx.size();
         }
      });

      log.trace("Releasing the gate");
      ccf.gate.open();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return TestingUtil.getTransactionTable(cache(2)).getRemoteTxCount() == 0;
         }
      });


      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            boolean allZero = true;
            for (int i = 0; i < 3; i++) {
               TransactionTable tt = TestingUtil.getTransactionTable(cache(i));
//               assertEquals("For cache " + i, 0, tt.getLocalTxCount());
//               assertEquals("For cache " + i, 0, tt.getRemoteTxCount());
               int local = tt.getLocalTxCount();
               int remote = tt.getRemoteTxCount();
               log.tracef("For cache %i , localTxCount=%s, remoteTxCount=%s", i, local, remote);
               log.tracef(String.format("For cache %s , localTxCount=%s, remoteTxCount=%s", i, local, remote));
               allZero = allZero && (local == 0);
               allZero = allZero && (remote == 0);
            }
            return allZero;
         }
      });

      for (Object key : keys2Tx.keySet()) {
         assertNotLocked(key);
         assertEquals(key, cache(0).get(key));
      }
   }

   private boolean keyMapsToNode(Object key, int nodeIndex) {
      Address owner = owner(key);
      return owner.equals(address(nodeIndex));
   }

   private Address owner(Object key) {
      return advancedCache(0).getDistributionManager().getConsistentHash().locatePrimaryOwner(key);
   }

}
