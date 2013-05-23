/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.tx.recovery.RecoveryDummyTransactionManagerLookup;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional")
public abstract class AbstractNoCrashTest extends MultipleCacheManagersTest {

   protected CacheMode cacheMode;
   protected LockingMode lockingMode;
   protected Boolean useSynchronization;

   protected AbstractNoCrashTest(CacheMode cacheMode, LockingMode lockingMode, Boolean useSynchronization) {
      this.cacheMode = cacheMode;
      this.lockingMode = lockingMode;
      this.useSynchronization = useSynchronization;
   }

   @Override
   protected final void createCacheManagers() {
      assert cacheMode != null && lockingMode != null && useSynchronization != null;
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(cacheMode, true);
      config.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new RecoveryDummyTransactionManagerLookup())
            .lockingMode(lockingMode)
            .useSynchronization(useSynchronization);
      config.clustering().hash().numOwners(3)
            .locking().lockAcquisitionTimeout(2000L);
      createCluster(config, 3);
      waitForClusterToForm();
   }

   protected interface Operation {
      void perform(Object key, int cacheIndex);
   }

   public void testTxAndLockOnDifferentNodesPut() throws Exception {
      testTxAndLockOnDifferentNodes(new Operation() {
         @Override
         public void perform(Object key, int cacheIndex) {
            cache(cacheIndex).put(key, "v");
         }
      }, false, false);
   }

   public void testTxAndLockOnDifferentNodesPutAll() throws Exception {
      testTxAndLockOnDifferentNodes(new Operation() {
         @Override
         public void perform(Object key, int cacheIndex) {
            cache(cacheIndex).putAll(Collections.singletonMap(key, "v"));
         }
      }, false, false);
   }

   public void testTxAndLockOnDifferentNodesReplace() throws Exception {
      testTxAndLockOnDifferentNodes(new Operation() {
         @Override
         public void perform(Object key, int cacheIndex) {
            cache(cacheIndex).replace(key, "v");
         }
      }, true, false);
   }

   public void testTxAndLockOnDifferentNodesRemove() throws Exception {
      testTxAndLockOnDifferentNodes(new Operation() {
         @Override
         public void perform(Object key, int cacheIndex) {
            cache(cacheIndex).remove(key);
         }
      }, true, true);
   }

   public void testTxAndLockOnSameNode() throws Exception {
      final Object k = getKeyForCache(0);

      tm(0).begin();
      cache(0).put(k, "v");
      DummyTransaction dtm = (DummyTransaction) tm(0).getTransaction();

      dtm.runPrepare();

      assert lockManager(0).isLocked(k);
      assert !lockManager(1).isLocked(k);
      assert !lockManager(2).isLocked(k);

      dtm.runCommitTx();

      assertNotLocked(k);
      assertValue(k, false);
   }

   protected abstract void testTxAndLockOnDifferentNodes(Operation operation, boolean addFirst, boolean removed) throws Exception;

   protected boolean noPendingTransactions(int i) {
      return checkTxCount(i, 0, 0);
   }

   protected void assertValue(Object k, boolean isRemove) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return noPendingTransactions(0) && noPendingTransactions(1) && noPendingTransactions(2);
         }
      });
      final String expected = isRemove ? null : "v";
      assertEquals(cache(0).get(k), expected);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return noPendingTransactions(0) && noPendingTransactions(1) && noPendingTransactions(2);
         }
      });


      assertEquals(cache(1).get(k), expected);
      assertEquals(cache(2).get(k), expected);
   }
}
