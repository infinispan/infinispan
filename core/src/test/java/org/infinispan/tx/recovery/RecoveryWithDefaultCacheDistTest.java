package org.infinispan.tx.recovery;

import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.xa.recovery.SerializableXid;
import org.testng.annotations.Test;

import javax.transaction.xa.Xid;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test (groups = "functional", testName = "tx.recovery.BasicRecoveryTest")
public class RecoveryWithDefaultCacheDistTest extends MultipleCacheManagersTest {

   Configuration configuration;

   @Override
   protected void createCacheManagers() throws Throwable {
      configuration = configure();
      createCluster(configuration, false, 2);
      waitForClusterToForm();

      //check that a default cache has been created
      manager(0).getCacheNames().contains(getRecoveryCacheName());
      manager(1).getCacheNames().contains(getRecoveryCacheName());
   }

   protected Configuration  configure() {
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      configuration.fluent().locking().useLockStriping(false);
      configuration.fluent().transaction()
         .transactionManagerLookupClass(DummyTransactionManagerLookup.class)
         .recovery();
      configuration.fluent().clustering().hash().rehashEnabled(false);
      return configuration;
   }

   public void testSimpleTx() throws Exception{
      tm(0).begin();
      cache(0).put("k","v");
      tm(0).commit();
      assert cache(1).get("k").equals("v");
   }

   public void testLocalAndRemoteTransaction() throws Exception {
      DummyTransaction t0 = RecoveryTestUtil.beginAndSuspendTx(cache(0));
      DummyTransaction t1 = RecoveryTestUtil.beginAndSuspendTx(cache(1));
      RecoveryTestUtil.assertPrepared(0, t0, t1);

      RecoveryTestUtil.prepareTransaction(t0);
      RecoveryTestUtil.assertPrepared(1, t0);
      RecoveryTestUtil.assertPrepared(0, t1);

      RecoveryTestUtil.prepareTransaction(t1);
      RecoveryTestUtil.assertPrepared(1, t0);
      RecoveryTestUtil.assertPrepared(1, t1);

      RecoveryTestUtil.commitTransaction(t0);
      RecoveryTestUtil.assertPrepared(0, t0);
      RecoveryTestUtil.assertPrepared(1, t1);

      RecoveryTestUtil.commitTransaction(t1);
      RecoveryTestUtil.assertPrepared(0, t0);
      RecoveryTestUtil.assertPrepared(0, t1);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            boolean noPrepTxOnFirstNode = cache(0, getRecoveryCacheName()).size() == 0;
            boolean noPrepTxOnSecondNode = cache(1, getRecoveryCacheName()).size() == 0;
            return noPrepTxOnFirstNode & noPrepTxOnSecondNode;
         }
      });
   }

   public void testNodeCrashesAfterPrepare() throws Exception {
      DummyTransaction t1_1 = RecoveryTestUtil.beginAndSuspendTx(cache(1));
      RecoveryTestUtil.prepareTransaction(t1_1);
      DummyTransaction t1_2 = RecoveryTestUtil.beginAndSuspendTx(cache(1));
      RecoveryTestUtil.prepareTransaction(t1_2);
      DummyTransaction t1_3 = RecoveryTestUtil.beginAndSuspendTx(cache(1));
      RecoveryTestUtil.prepareTransaction(t1_3);

      assertEquals(cache(0, getRecoveryCacheName()).size(), 3);
      manager(1).stop();
      super.cacheManagers.remove(1);
      TestingUtil.blockUntilViewReceived(cache(0), 1, 60000);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return RecoveryTestUtil.rm(cache(0)).getLocalInDoubtTransactions().size() == 3;
         }
      });

      List<Xid> inDoubtTransactions = RecoveryTestUtil.rm(cache(0)).getLocalInDoubtTransactions();
      assertEquals(3, inDoubtTransactions.size());
      assert inDoubtTransactions.contains(new SerializableXid(t1_1.getXid()));
      assert inDoubtTransactions.contains(new SerializableXid(t1_2.getXid()));
      assert inDoubtTransactions.contains(new SerializableXid(t1_3.getXid()));

      addClusterEnabledCacheManager(configuration, false);
      defineRecoveryCache(1);
      TestingUtil.blockUntilViewsReceived(60000, cache(0), cache(1));
      DummyTransaction t1_4 = RecoveryTestUtil.beginAndSuspendTx(cache(1));
      RecoveryTestUtil.prepareTransaction(t1_4);
      log.trace("Before main recovery call.");
      RecoveryTestUtil.assertPrepared(4, t1_4);

      //now second call would only return 1 prepared tx as we only go over the network once
      RecoveryTestUtil.assertPrepared(1, t1_4);

      RecoveryTestUtil.commitTransaction(t1_4);
      RecoveryTestUtil.assertPrepared(0, t1_4);

      inDoubtTransactions = RecoveryTestUtil.rm(cache(0)).getLocalInDoubtTransactions();
      assertEquals(3, inDoubtTransactions.size());
      assert inDoubtTransactions.contains(new SerializableXid(t1_1.getXid()));
      assert inDoubtTransactions.contains(new SerializableXid(t1_2.getXid()));
      assert inDoubtTransactions.contains(new SerializableXid(t1_3.getXid()));


      //now let's start to forget transactions
      t1_4.firstEnlistedResource().forget(t1_1.getXid());
      log.info("returned");
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return RecoveryTestUtil.rm(cache(0)).getLocalInDoubtTransactions().size() == 2;
         }
      });
      inDoubtTransactions = RecoveryTestUtil.rm(cache(0)).getLocalInDoubtTransactions();
      assertEquals(2, inDoubtTransactions.size());
      assert inDoubtTransactions.contains(new SerializableXid(t1_2.getXid()));
      assert inDoubtTransactions.contains(new SerializableXid(t1_3.getXid()));

      t1_4.firstEnlistedResource().forget(t1_2.getXid());
      t1_4.firstEnlistedResource().forget(t1_3.getXid());
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return RecoveryTestUtil.rm(cache(0)).getLocalInDoubtTransactions().size() == 0;
         }
      });
      assertEquals(0, RecoveryTestUtil.rm(cache(0)).getLocalInDoubtTransactions().size());
   }

   protected void defineRecoveryCache(int cacheManagerIndex) {
   }

   protected String getRecoveryCacheName() {
      return Configuration.RecoveryType.DEFAULT_RECOVERY_INFO_CACHE;
   }
}
