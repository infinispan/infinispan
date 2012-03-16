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

import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import javax.transaction.Status;

import static org.testng.Assert.assertEquals;


/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.MinViewIdCalculusTest")
@CleanupAfterMethod
public class MinViewIdCalculusTest extends MultipleCacheManagersTest {

   private Configuration c;

   @Override
   protected void createCacheManagers() throws Throwable {
      c = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      c.fluent()
            .transaction()
               .lockingMode(LockingMode.PESSIMISTIC)
               .transactionManagerLookup(new DummyTransactionManagerLookup());
      c.fluent().hash().numOwners(3);
      createCluster(c, 2);
      waitForClusterToForm();
   }

   public void testMinViewId1() throws Exception {
      final TransactionTable tt0 = TestingUtil.getTransactionTable(cache(0));
      final TransactionTable tt1 = TestingUtil.getTransactionTable(cache(1));

      final int viewId = advancedCache(0).getRpcManager().getTransport().getViewId();
      assertEquals(tt0.getMinViewId(), viewId);
      assertEquals(tt1.getMinViewId(), viewId);

      //add a new cache and check that min view is updated
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();

      final int viewId2 = advancedCache(0).getRpcManager().getTransport().getViewId();
      assertEquals(viewId + 1, viewId2);

      assertEquals(tt0.getMinViewId(), viewId2);
      assertEquals(tt1.getMinViewId(), viewId2);

      final TransactionTable tt2 = TestingUtil.getTransactionTable(cache(1));
      assertEquals(tt2.getMinViewId(), viewId2);
   }

   public void testMinViewId2() throws Exception {

      final TransactionTable tt0 = TestingUtil.getTransactionTable(cache(0));
      final TransactionTable tt1 = TestingUtil.getTransactionTable(cache(1));


      final int viewId = advancedCache(0).getRpcManager().getTransport().getViewId();

      tm(1).begin();
      cache(1).put(getKeyForCache(0),"v");
      final DummyTransaction t = (DummyTransaction) tm(1).getTransaction();
      t.runPrepare();
      tm(1).suspend();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 1);
         }
      });

      log.trace("Here is the topology change..");
      //add a new cache and check that min view is updated
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();

      final int viewId2 = advancedCache(0).getRpcManager().getTransport().getViewId();
      assertEquals(viewId + 1, viewId2);

      assertEquals(tt0.getMinViewId(), viewId);
      assertEquals(tt1.getMinViewId(), viewId);

      tm(1).resume(t);
      t.runCommitTx();


      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return tt0.getMinViewId() == viewId2 && tt1.getMinViewId() == viewId2;
         }
      });
   }
}
