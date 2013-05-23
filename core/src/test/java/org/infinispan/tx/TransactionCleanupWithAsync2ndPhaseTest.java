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

package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "tx.TransactionCleanupWithAsync2ndPhaseTest")
public class TransactionCleanupWithAsync2ndPhaseTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getConfiguration();
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   protected ConfigurationBuilder getConfiguration() {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      dcc.transaction().syncCommitPhase(false).syncRollbackPhase(true);
      return dcc;
   }

   public void transactionCleanupTest1() throws Throwable {
      runtTest(1, false);
   }

   public void transactionCleanupTest2() throws Throwable {
      runtTest(0, false);
   }

   public void transactionCleanupTest3() throws Throwable {
      runtTest(1, true);
   }

   public void transactionCleanupTest4() throws Throwable {
      runtTest(0, true);
   }

   private void runtTest(int initiatorIndex, boolean rollback) throws Throwable{
      tm(initiatorIndex).begin();
      cache(initiatorIndex).put("k", "v");
      if (rollback) {
         tm(initiatorIndex).rollback();
      } else {
         tm(initiatorIndex).commit();
      }

      assertNotLocked("k");
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return (TestingUtil.getTransactionTable(cache(0)).getRemoteTxCount() == 0) &&
            (TestingUtil.getTransactionTable(cache(0)).getLocalTxCount() == 0) &&
            (TestingUtil.getTransactionTable(cache(1)).getRemoteTxCount() == 0) &&
            (TestingUtil.getTransactionTable(cache(1)).getLocalTxCount() == 0);
         }
      });
   }
}

