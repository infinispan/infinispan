/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Test if state transfer happens properly on a cache with pessimistic transactions.
 * See https://issues.jboss.org/browse/ISPN-2408.
 *
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.StateTransferPessimisticTest")
@CleanupAfterMethod
public class StateTransferPessimisticTest extends MultipleCacheManagersTest {

   public static final int NUM_KEYS = 100;
   private ConfigurationBuilder dccc;

   @Override
   protected void createCacheManagers() throws Throwable {
      dccc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true, true);
      dccc.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new DummyTransactionManagerLookup())
            .lockingMode(LockingMode.PESSIMISTIC)
            .syncCommitPhase(true)
            .syncRollbackPhase(true);
      dccc.clustering().hash().numOwners(1).l1().disable().onRehash(false).locking().lockAcquisitionTimeout(1000l);
      dccc.clustering().stateTransfer().fetchInMemoryState(true);
      createCluster(dccc, 2);
      waitForClusterToForm();
   }

   public void testStateTransfer() throws Exception {
      Set<Object> keys = new HashSet<Object>();
      for (int i = 0; i < NUM_KEYS; i++) {
         Object key = getKeyForCache(0);
         if (!keys.add(key)) continue;

         // put a key to have some data in cache
         cache(0).put(key, key);
      }

      log.trace("State transfer happens here");
      // add a third node
      addClusterEnabledCacheManager(dccc);
      waitForClusterToForm();

      log.trace("Checking the values from caches...");
      for (Object key : keys) {
         log.tracef("Checking key: %s", key);
         // check them directly in data container
         InternalCacheEntry d0 = advancedCache(0).getDataContainer().get(key);
         InternalCacheEntry d1 = advancedCache(1).getDataContainer().get(key);
         InternalCacheEntry d2 = advancedCache(2).getDataContainer().get(key);
         int c = 0;
         if (d0 != null && !d0.isExpired(TIME_SERVICE.wallClockTime())) {
            assertEquals(key, d0.getValue());
            c++;
         }
         if (d1 != null && !d1.isExpired(TIME_SERVICE.wallClockTime())) {
            assertEquals(key, d1.getValue());
            c++;
         }
         if (d2 != null && !d2.isExpired(TIME_SERVICE.wallClockTime())) {
            assertEquals(key, d2.getValue());
            c++;
         }
         assertEquals(1, c);

         // look at them also via cache API
         assertEquals(key, cache(0).get(key));
         assertEquals(key, cache(1).get(key));
         assertEquals(key, cache(2).get(key));
      }
   }
}
