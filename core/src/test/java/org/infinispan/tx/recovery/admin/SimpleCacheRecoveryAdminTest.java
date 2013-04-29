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
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.tx.recovery.RecoveryDummyTransactionManagerLookup;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.transaction.xa.Xid;
import java.util.List;

import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.tx.recovery.RecoveryTestUtil.beginAndSuspendTx;
import static org.infinispan.tx.recovery.RecoveryTestUtil.prepareTransaction;
import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.recovery.admin.SimpleCacheRecoveryAdminTest")
@CleanupAfterMethod
public class SimpleCacheRecoveryAdminTest extends AbstractRecoveryTest {

   private MBeanServer threadMBeanServer;
   private static final String JMX_DOMAIN = "tx.recovery.admin.LocalCacheRecoveryAdminTest";
   private DummyTransaction tx1;

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.fluent()
            .globalJmxStatistics()
            .mBeanServerLookup(new PerThreadMBeanServerLookup())
            .jmxDomain(JMX_DOMAIN).allowDuplicateDomains(true);
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true).fluent()
            .transaction().transactionManagerLookupClass(RecoveryDummyTransactionManagerLookup.class).recovery()
            .jmxStatistics()
            .locking().useLockStriping(false)
            .clustering().hash().numOwners(3)
            .clustering().l1().disable()
            .build();
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createCacheManager(globalConfiguration, configuration, true);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createCacheManager(globalConfiguration, configuration, true);
      EmbeddedCacheManager cm3 = TestCacheManagerFactory.createCacheManager(globalConfiguration, configuration, true);
      registerCacheManager(cm1);
      registerCacheManager(cm2);
      registerCacheManager(cm3);
      cache(0, "test");
      cache(1, "test");
      cache(2, "test");

      TestingUtil.waitForRehashToComplete(caches("test"));

      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();

      assert showInDoubtTransactions(0).isEmpty();
      assert showInDoubtTransactions(1).isEmpty();
      assert showInDoubtTransactions(2).isEmpty();

      tx1 = beginAndSuspendTx(cache(2, "test"));
      prepareTransaction(tx1);

      log.trace("Shutting down a cache " + address(cache(2, "test")));

      TestingUtil.killCacheManagers(manager(2));
      TestingUtil.blockUntilViewsReceived(90000, false, cache(0, "test"), cache(1, "test"));

   }

   public void testJmxOperationMetadata() throws Exception {
      checkMBeanOperationParameterNaming(getRecoveryAdminObjectName(0));
   }

   public void testForceCommitOnOtherNode() throws Exception {
      String inDoubt = showInDoubtTransactions(0);

      assertInDoubtTxCount(inDoubt, 1);
      assertInDoubtTxCount(showInDoubtTransactions(1), 1);
      List<Long> ids = getInternalIds(inDoubt);

      assertEquals(1, ids.size());
      assertEquals(0, cache(0, "test").keySet().size());
      assertEquals(0, cache(1, "test").keySet().size());

      if (log.isTraceEnabled()) log.trace("Before forcing commit!");
      String result = invokeForceWithId("forceCommit", 0, ids.get(0));

      checkResponse(result, 1);
   }

   public void testForceCommitXid() {
      String s = invokeForceWithXid("forceCommit", 0, tx1.getXid());
      System.out.println("s = " + s);
      checkResponse(s, 1);

      //try again
      s = invokeForceWithXid("forceCommit", 0, tx1.getXid());
      assert s.indexOf("Transaction not found") >= 0;
   }

   public void testForceRollbackInternalId() {
      List<Long> ids = getInternalIds(showInDoubtTransactions(0));
      log.tracef("test:: invoke rollback for %s", ids);
      String result = invokeForceWithId("forceRollback", 0, ids.get(0));

      checkResponse(result, 0);

      assert invokeForceWithId("forceRollback", 0, ids.get(0)).contains("Transaction not found");
   }

   public void testForceRollbackXid() {
      String s = invokeForceWithXid("forceRollback", 0, tx1.getXid());
      checkResponse(s, 0);

      //try again
      s = invokeForceWithXid("forceRollback", 0, tx1.getXid());
      assert s.indexOf("Transaction not found") >= 0;
   }

   private void checkResponse(String result, int entryCount) {
      assert isSuccess(result) : "Received: " + result;

      assertEquals(cache(0, "test").keySet().size(), entryCount);
      assertEquals(cache(1, "test").keySet().size(), entryCount);


      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return showInDoubtTransactions(0).isEmpty() && showInDoubtTransactions(1).isEmpty();
         }
      });

      //just make sure everything is cleaned up properly now
      checkProperlyCleanup(0);
      checkProperlyCleanup(1);
   }


   @Override
   protected void checkProperlyCleanup(final int managerIndex) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return TestingUtil.extractLockManager(cache(managerIndex, "test")).getNumberOfLocksHeld() == 0;
         }
      });
      final TransactionTable tt = TestingUtil.extractComponent(cache(managerIndex, "test"), TransactionTable.class);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return (tt.getRemoteTxCount() == 0) && (tt.getLocalTxCount() == 0);
         }
      });
      final RecoveryManager rm = TestingUtil.extractComponent(cache(managerIndex, "test"), RecoveryManager.class);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return rm.getInDoubtTransactions().size() == 0;
         }
      });
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return rm.getPreparedTransactionsFromCluster().all().length == 0;
         }
      });
   }


   private String invokeForceWithId(String methodName, int cacheIndex, Long aLong) {
      try {
         ObjectName recoveryAdmin = getRecoveryAdminObjectName(cacheIndex);
         return threadMBeanServer.invoke(recoveryAdmin, methodName, new Object[]{aLong}, new String[]{long.class.getName()}).toString();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private String invokeForceWithXid(String methodName, int cacheIndex, Xid xid) {
      try {
         ObjectName recoveryAdmin = getRecoveryAdminObjectName(cacheIndex);
         Object[] params = {xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier()};
         String[] signature = {int.class.getName(), byte[].class.getName(), byte[].class.getName()};
         return threadMBeanServer.invoke(recoveryAdmin, methodName, params, signature).toString();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private void assertInDoubtTxCount(String inDoubt, int expectedCount) {
      int count = countInDoubtTx(inDoubt);
      assertEquals(expectedCount, count);
   }

   private String showInDoubtTransactions(int cacheIndex) {
      try {
         ObjectName recoveryAdmin = getRecoveryAdminObjectName(cacheIndex);

         return (String) threadMBeanServer.invoke(recoveryAdmin, "showInDoubtTransactions", new Object[0], new String[0]);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private ObjectName getRecoveryAdminObjectName(int cacheIndex) throws Exception {
      String postfix = cacheIndex == 0 ? "" : String.valueOf(++cacheIndex);
      return getCacheObjectName(JMX_DOMAIN + postfix, "test(dist_sync)", "RecoveryAdmin");
   }
}
