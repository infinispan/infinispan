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

package org.infinispan.tx.recovery.admin;

import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.xa.recovery.RecoveryAdminOperations;
import org.infinispan.transaction.xa.recovery.RecoveryAwareTransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.0
 */
public abstract class AbstractRecoveryTest extends MultipleCacheManagersTest {

   protected Configuration defaultRecoveryConfig() {
      return getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, false).fluent()
            .transaction().transactionManagerLookupClass(DummyTransactionManagerLookup.class).recovery()
            .locking().useLockStriping(false)
            .clustering().hash().numOwners(2).rehashEnabled(false)
            .clustering().l1().disable().stateRetrieval().fetchInMemoryState(false)
            .build();
   }

   protected int countInDoubtTx(String inDoubt) {
      int lastIndex = 0;
      int count = 0;
      while ((lastIndex = inDoubt.indexOf("internalId", lastIndex + 1)) >= 0) {
         count ++;
      }
      return count;
   }

   protected RecoveryAdminOperations recoveryOps(int cacheIndex) {
      return advancedCache(cacheIndex).getComponentRegistry().getComponent(RecoveryAdminOperations.class);
   }

   protected List<Long> getInternalIds(String inDoubt) {
      Pattern p = Pattern.compile("internalId = [0-9]*");
      Matcher matcher = p.matcher(inDoubt);
      List<Long> result = new ArrayList<Long>();
      while (matcher.find()) {
         String group = matcher.group();
         Long id = Long.parseLong(group.substring("internalId = ".length()));
         result.add(id);
      }
      return result;
   }

   protected boolean isSuccess(String result) {
      return result.contains("successful");
   }

   public RecoveryAwareTransactionTable tt(int index) {
      return (RecoveryAwareTransactionTable) advancedCache(index).getComponentRegistry().getComponent(TransactionTable.class);
   }

   protected void checkProperlyCleanup(int managerIndex) {
      assertEquals(TestingUtil.extractLockManager(cache(managerIndex)).getNumberOfLocksHeld(), 0, "For cache " + address(managerIndex) + "(" + managerIndex + ')');
      final TransactionTable tt = TestingUtil.extractComponent(cache(managerIndex), TransactionTable.class);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return (tt.getRemoteTxCount() == 0) && (tt.getLocalTxCount() == 0);
         }
      });
      final RecoveryManager rm = TestingUtil.extractComponent(cache(managerIndex), RecoveryManager.class);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return rm.getInDoubtTransactions().isEmpty() && rm.getPreparedTransactionsFromCluster().all().length == 0;
         }
      });
   }

   protected void assertCleanup(int... caches) {
      for (int i : caches) {
         checkProperlyCleanup(i);
      }
   }

   public RecoveryManagerImpl recoveryManager(int cacheIndex) {
      return (RecoveryManagerImpl) TestingUtil.extractComponent(cache(cacheIndex), RecoveryManager.class);
   }

   protected int getTxParticipant(boolean txParticipant) {
      int expectedNumber = txParticipant ? 1 : 0;

      int index = -1;
      for (int i = 0; i < 2; i++) {
         if (recoveryManager(i).getInDoubtTransactionsMap().size() == expectedNumber) {
            index = i;
            break;
         }
      }
      return index;
   }

}
