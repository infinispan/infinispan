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

package org.infinispan.tx.recovery;

import org.infinispan.config.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryAwareTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.xa.Xid;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static org.infinispan.tx.recovery.RecoveryTestUtil.beginAndSuspendTx;
import static org.infinispan.tx.recovery.RecoveryTestUtil.commitTransaction;
import static org.infinispan.tx.recovery.RecoveryTestUtil.prepareTransaction;


/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.recovery.PostCommitRecoveryStateTest")
public class PostCommitRecoveryStateTest extends MultipleCacheManagersTest {

   private static Log log = LogFactory.getLog(PostCommitRecoveryStateTest.class);

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      configuration.fluent().locking().useLockStriping(false);
      configuration.fluent().transaction()
         .transactionManagerLookupClass(DummyTransactionManagerLookup.class)
         .recovery();
      configuration.fluent().clustering().hash().rehashEnabled(false);
      createCluster(configuration, 2);
      waitForClusterToForm();
      ComponentRegistry componentRegistry = this.cache(0).getAdvancedCache().getComponentRegistry();
      XaTransactionTable txTable = (XaTransactionTable) componentRegistry.getComponent(TransactionTable.class);
      txTable.setRecoveryManager(new RecoveryManagerDelegate(txTable.getRecoveryManager()));
   }

   public void testState() throws Exception {

      RecoveryManagerImpl rm1 = (RecoveryManagerImpl) advancedCache(1).getComponentRegistry().getComponent(RecoveryManager.class);
      TransactionTable tt1 = advancedCache(1).getComponentRegistry().getComponent(TransactionTable.class);
      assertEquals(rm1.getInDoubtTransactionsMap().size(), 0);
      assertEquals(tt1.getRemoteTxCount(), 0);

      DummyTransaction t0 = beginAndSuspendTx(cache(0));
      assertEquals(rm1.getInDoubtTransactionsMap().size(),0);
      assertEquals(tt1.getRemoteTxCount(), 0);

      prepareTransaction(t0);
      assertEquals(rm1.getInDoubtTransactionsMap().size(),0);
      assertEquals(tt1.getRemoteTxCount(), 1);

      commitTransaction(t0);
      assertEquals(tt1.getRemoteTxCount(), 1);
      assertEquals(rm1.getInDoubtTransactionsMap().size(), 0);
   }

   public static class RecoveryManagerDelegate implements RecoveryManager {
      volatile RecoveryManager rm;

      public boolean swallowRemoveRecoveryInfoCalls = true;

      public RecoveryManagerDelegate(RecoveryManager recoveryManager) {
         this.rm = recoveryManager;
      }

      @Override
      public RecoveryIterator getPreparedTransactionsFromCluster() {
         return rm.getPreparedTransactionsFromCluster();
      }

      @Override
      public void removeRecoveryInformationFromCluster(Collection<Address> where, Xid xid, boolean sync, GlobalTransaction gtx) {
         if (swallowRemoveRecoveryInfoCalls){
            log.trace("PostCommitRecoveryStateTest$RecoveryManagerDelegate.removeRecoveryInformation");
         } else {
            this.rm.removeRecoveryInformationFromCluster(where, xid, sync, null);
         }
      }

      @Override
      public void removeRecoveryInformationFromCluster(Collection<Address> where, long internalId, boolean sync) {
         rm.removeRecoveryInformationFromCluster(where, internalId, sync);
      }

      @Override
      public RecoveryAwareTransaction removeRecoveryInformation(Xid xid) {
         rm.removeRecoveryInformation(xid);
         return null;
      }

      @Override
      public Set<InDoubtTxInfo> getInDoubtTransactionInfoFromCluster() {
         return rm.getInDoubtTransactionInfoFromCluster();
      }

      @Override
      public Set<InDoubtTxInfo> getInDoubtTransactionInfo() {
         return rm.getInDoubtTransactionInfo();
      }

      @Override
      public List<Xid> getInDoubtTransactions() {
         return rm.getInDoubtTransactions();
      }

      @Override
      public RecoveryAwareTransaction getPreparedTransaction(Xid xid) {
         return rm.getPreparedTransaction(xid);
      }

      @Override
      public String forceTransactionCompletion(Xid xid, boolean commit) {
         return rm.forceTransactionCompletion(xid, commit);
      }

      @Override
      public String forceTransactionCompletionFromCluster(Xid xid, Address where, boolean commit) {
         return rm.forceTransactionCompletionFromCluster(xid, where, commit);
      }

      @Override
      public boolean isTransactionPrepared(GlobalTransaction globalTx) {
         return rm.isTransactionPrepared(globalTx);
      }

      @Override
      public RecoveryAwareTransaction removeRecoveryInformation(Long internalId) {
         rm.removeRecoveryInformation(internalId);
         return null;
      }
   }
}
