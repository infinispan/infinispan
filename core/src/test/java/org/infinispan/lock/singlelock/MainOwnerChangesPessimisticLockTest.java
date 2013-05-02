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

package org.infinispan.lock.singlelock;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.TimeService;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Main owner changes due to state transfer in a distributed cluster using pessimistic locking.
 *
 * @since 5.2
 */
@Test(groups = "functional", testName = "lock.singlelock.MainOwnerChangesPessimisticLockTest")
@CleanupAfterMethod
public class MainOwnerChangesPessimisticLockTest extends MultipleCacheManagersTest {

   public static final int NUM_KEYS = 10;
   private ConfigurationBuilder dccc;

   @Override
   protected void createCacheManagers() throws Throwable {
      dccc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true, true);
      dccc.transaction()
            .transactionManagerLookup(new DummyTransactionManagerLookup())
            .lockingMode(LockingMode.PESSIMISTIC)
            .syncCommitPhase(true)
            .syncRollbackPhase(true)
            .locking().lockAcquisitionTimeout(1000l)
            .clustering().hash().numOwners(1).numSegments(3)
            .l1().disable().onRehash(false)
            .stateTransfer().fetchInMemoryState(true);
      createCluster(dccc, 2);
      waitForClusterToForm();
   }

   public void testLocalLockMigrationTxCommit() throws Exception {
      testLockMigration(0, true);
   }

   public void testLocalLockMigrationTxRollback() throws Exception {
      testLockMigration(0, false);
   }

   public void testRemoteLockMigrationTxCommit() throws Exception {
      testLockMigration(1, true);
   }

   public void testRemoteLockMigrationTxRollback() throws Exception {
      testLockMigration(1, false);
   }

   private void testLockMigration(int nodeThatPuts, boolean commit) throws Exception {
      Map<Object, Transaction> key2Tx = new HashMap<Object, Transaction>();
      for (int i = 0; i < NUM_KEYS; i++) {
         Object key = getKeyForCache(0);
         if (key2Tx.containsKey(key)) continue;

         // put a key to have some data in cache
         cache(nodeThatPuts).put(key, key);

         // start a TX that locks the key and then we suspend it
         tm(nodeThatPuts).begin();
         Transaction tx = tm(nodeThatPuts).getTransaction();
         advancedCache(nodeThatPuts).lock(key);
         tm(nodeThatPuts).suspend();
         key2Tx.put(key, tx);

         assertLocked(0, key);
      }

      log.trace("Lock transfer happens here");

      // add a third node hoping that some of the previously created keys will be migrated to it
      addClusterEnabledCacheManager(dccc);
      waitForClusterToForm();

      // search for a key that was migrated to third node and the suspended TX that locked it
      Object migratedKey = null;
      Transaction migratedTransaction = null;
      ConsistentHash consistentHash = advancedCache(2).getDistributionManager().getConsistentHash();
      for (Object key : key2Tx.keySet()) {
         if (consistentHash.locatePrimaryOwner(key).equals(address(2))) {
            migratedKey = key;
            migratedTransaction = key2Tx.get(key);
            log.trace("Migrated key = " + migratedKey);
            log.trace("Migrated transaction = " + ((DummyTransaction) migratedTransaction).getEnlistedResources());
            break;
         }
      }

      // we do not focus on the other transactions so we commit them now
      log.trace("Committing all transactions except the migrated one.");
      for (Object key : key2Tx.keySet()) {
         if (!key.equals(migratedKey)) {
            Transaction tx = key2Tx.get(key);
            tm(nodeThatPuts).resume(tx);
            tm(nodeThatPuts).commit();
         }
      }

      if (migratedKey == null) {
         // this could happen in extreme cases
         log.trace("No key migrated to new owner - test cannot be performed!");
      } else {
         // the migrated TX is resumed and committed or rolled back. we expect the migrated key to be unlocked now
         tm(nodeThatPuts).resume(migratedTransaction);
         if (commit) {
            tm(nodeThatPuts).commit();
         } else {
            tm(nodeThatPuts).rollback();
         }

         // there should not be any locks
         assertNotLocked(cache(0), migratedKey);
         assertNotLocked(cache(1), migratedKey);
         assertNotLocked(cache(2), migratedKey);

         // if a new TX tries to write to the migrated key this should not fail, the key should not be locked
         tm(nodeThatPuts).begin();
         cache(nodeThatPuts).put(migratedKey, "someValue"); // this should not result in TimeoutException due to key still locked
         tm(nodeThatPuts).commit();
      }

      log.trace("Checking the values from caches...");
      for (Object key : key2Tx.keySet()) {
         log.tracef("Checking key: %s", key);
         Object expectedValue = key;
         if (key.equals(migratedKey)) {
            expectedValue = "someValue";
         }
         // check them directly in data container
         InternalCacheEntry d0 = advancedCache(0).getDataContainer().get(key);
         InternalCacheEntry d1 = advancedCache(1).getDataContainer().get(key);
         InternalCacheEntry d2 = advancedCache(2).getDataContainer().get(key);
         int c = 0;
         if (d0 != null && !d0.isExpired(TIME_SERVICE.wallClockTime())) {
            assertEquals(expectedValue, d0.getValue());
            c++;
         }
         if (d1 != null && !d1.isExpired(TIME_SERVICE.wallClockTime())) {
            assertEquals(expectedValue, d1.getValue());
            c++;
         }
         if (d2 != null && !d2.isExpired(TIME_SERVICE.wallClockTime())) {
            assertEquals(expectedValue, d2.getValue());
            c++;
         }
         assertEquals(1, c);

         // look at them also via cache API
         assertEquals(expectedValue, cache(0).get(key));
         assertEquals(expectedValue, cache(1).get(key));
         assertEquals(expectedValue, cache(2).get(key));
      }
   }
}
