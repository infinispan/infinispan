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

package org.infinispan.tx.locking;

import javax.transaction.Transaction;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.locking.OptimisticReplTxTest")
public class OptimisticReplTxTest extends AbstractClusteredTxTest {

  CacheMode cacheMode;


   public OptimisticReplTxTest() {
      this.cacheMode = CacheMode.REPL_SYNC;
   }

   // check that two transactions can progress in parallel on the same node
   public void testTxProgress() throws Exception {
      tm(0).begin();
      cache(0).put(k, "v1");
      final Transaction tx1 = tm(0).suspend();

      //another tx working on the same keys
      tm(0).begin();
      cache(0).put(k, "v2");
      tm(0).commit();
      assert cache(0).get(k).equals("v2");
      assert cache(1).get(k).equals("v2");

      tm(0).resume(tx1);
      tm(0).commit();
      assert cache(0).get(k).equals("v1");
      assert cache(1).get(k).equals("v1");
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      k = "k";
      final ConfigurationBuilder conf = getDefaultClusteredCacheConfig(cacheMode, true);
      conf.transaction()
            .lockingMode(LockingMode.OPTIMISTIC)
            .transactionManagerLookup(new DummyTransactionManagerLookup());
      createCluster(conf, 2);
      waitForClusterToForm();
   }

   @Override
   protected void assertLocking() {
      assertFalse(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
      prepare();
      assertTrue(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
      commit();
      assertFalse(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
   }

   @Override
   protected void assertLockingNoChanges() {
      assertFalse(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
      prepare();
      assertFalse(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
      commit();
      assertFalse(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
   }

   @Override
   protected void assertLockingOnRollback() {
      assertFalse(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
      rollback();
      assertFalse(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
   }
}
