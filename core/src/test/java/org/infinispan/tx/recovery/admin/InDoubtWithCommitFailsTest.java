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

import org.infinispan.CacheException;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.tx.recovery.RecoveryDummyTransactionManagerLookup;
import org.testng.annotations.Test;

import static org.infinispan.tx.recovery.RecoveryTestUtil.*;
import static org.testng.Assert.assertEquals;

/**
 * This test makes sure that when a transaction fails during commit it is reported as in-doubt transaction.
 *
 * @author Mircea Markus
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.recovery.admin.InDoubtWithCommitFailsTest")
@CleanupAfterMethod
public class InDoubtWithCommitFailsTest extends AbstractRecoveryTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      configuration.transaction().transactionManagerLookup(new RecoveryDummyTransactionManagerLookup())
            .useSynchronization(false)
            .recovery().enable()
            .locking().useLockStriping(false)
            .clustering().hash().numOwners(2)
            .clustering().l1().disable().stateTransfer().fetchInMemoryState(false);
      createCluster(configuration, 2);
      waitForClusterToForm();
      advancedCache(1).addInterceptorBefore(new ForceFailureInterceptor(), InvocationContextInterceptor.class);
   }

   public void testRecoveryInfoListCommit() throws Exception {
      test(true);
   }

   public void testRecoveryInfoListRollback() throws Exception {
      test(false);
   }

   private void test(boolean commit) {
      assert recoveryOps(0).showInDoubtTransactions().isEmpty();
      TransactionTable tt0 = cache(0).getAdvancedCache().getComponentRegistry().getComponent(TransactionTable.class);

      DummyTransaction dummyTransaction = beginAndSuspendTx(cache(0));
      prepareTransaction(dummyTransaction);
      assert tt0.getLocalTxCount() == 1;

      try {
         if (commit) {
            commitTransaction(dummyTransaction);
         } else {
            rollbackTransaction(dummyTransaction);
         }
         assert false : "exception expected";
      } catch (Exception e) {
         //expected
      }
      assertEquals(tt0.getLocalTxCount(), 1);


      assertEquals(countInDoubtTx(recoveryOps(0).showInDoubtTransactions()), 1);
      assertEquals(countInDoubtTx(recoveryOps(1).showInDoubtTransactions()), 1);
   }

   public static class ForceFailureInterceptor extends CommandInterceptor {

      public boolean fail = true;

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         fail();
         return super.visitCommitCommand(ctx, command);
      }

      private void fail() {
         if (fail) throw new CacheException("Induced failure");
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         fail();
         return super.visitRollbackCommand(ctx, command);
      }

   }
}
