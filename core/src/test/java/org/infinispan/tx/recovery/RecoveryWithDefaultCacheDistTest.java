package org.infinispan.tx.recovery;

import static org.infinispan.tx.recovery.RecoveryTestUtil.assertPrepared;
import static org.infinispan.tx.recovery.RecoveryTestUtil.beginAndSuspendTx;
import static org.infinispan.tx.recovery.RecoveryTestUtil.commitTransaction;
import static org.infinispan.tx.recovery.RecoveryTestUtil.prepareTransaction;
import static org.infinispan.tx.recovery.RecoveryTestUtil.rm;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.RecoveryConfiguration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.transaction.xa.recovery.SerializableXid;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test (groups = "functional", testName = "tx.recovery.RecoveryWithDefaultCacheDistTest")
@CleanupAfterMethod
public class RecoveryWithDefaultCacheDistTest extends MultipleCacheManagersTest {

   protected ConfigurationBuilder configuration;

   @Override
   protected void createCacheManagers() throws Throwable {
      configuration = configure();
      createCluster(configuration, 2);
      waitForClusterToForm();

      //check that a default cache has been created
      manager(0).getCacheNames().contains(getRecoveryCacheName());
      manager(1).getCacheNames().contains(getRecoveryCacheName());
   }

   protected ConfigurationBuilder configure() {
      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      configuration.locking().useLockStriping(false)
            .transaction().transactionManagerLookup(new RecoveryDummyTransactionManagerLookup())
            .useSynchronization(false)
            .recovery().enable()
            .clustering().stateTransfer().fetchInMemoryState(false);
      return configuration;
   }

   public void testSimpleTx() throws Exception{
      tm(0).begin();
      cache(0).put("k","v");
      tm(0).commit();
      assert cache(1).get("k").equals("v");
   }

   public void testLocalAndRemoteTransaction() throws Exception {
      DummyTransaction t0 = beginAndSuspendTx(cache(0));
      DummyTransaction t1 = beginAndSuspendTx(cache(1));
      assertPrepared(0, t0, t1);

      prepareTransaction(t0);
      assertPrepared(1, t0);
      assertPrepared(0, t1);

      prepareTransaction(t1);
      assertPrepared(1, t0);
      assertPrepared(1, t1);

      commitTransaction(t0);
      assertPrepared(0, t0);
      assertPrepared(1, t1);

      commitTransaction(t1);
      assertPrepared(0, t0);
      assertPrepared(0, t1);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            boolean noPrepTxOnFirstNode = cache(0, getRecoveryCacheName()).size() == 0;
            boolean noPrepTxOnSecondNode = cache(1, getRecoveryCacheName()).size() == 0;
            return noPrepTxOnFirstNode & noPrepTxOnSecondNode;
         }
      });

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            final Set<RecoveryManager.InDoubtTxInfo> inDoubt = rm(cache(0)).getInDoubtTransactionInfo();
            return inDoubt.size() == 0;
         }
      });
   }

   public void testNodeCrashesAfterPrepare() throws Exception {
      DummyTransaction t1_1 = beginAndSuspendTx(cache(1));
      prepareTransaction(t1_1);
      DummyTransaction t1_2 = beginAndSuspendTx(cache(1));
      prepareTransaction(t1_2);
      DummyTransaction t1_3 = beginAndSuspendTx(cache(1));
      prepareTransaction(t1_3);

      manager(1).stop();
      super.cacheManagers.remove(1);
      TestingUtil.blockUntilViewReceived(cache(0), 1, 60000, false);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            int size = rm(cache(0)).getInDoubtTransactionInfo().size();
            return size == 3;
         }
      });

      List<Xid> inDoubtTransactions = rm(cache(0)).getInDoubtTransactions();
      assertEquals(3, inDoubtTransactions.size());
      assert inDoubtTransactions.contains(new SerializableXid(t1_1.getXid()));
      assert inDoubtTransactions.contains(new SerializableXid(t1_2.getXid()));
      assert inDoubtTransactions.contains(new SerializableXid(t1_3.getXid()));

      configuration.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      addClusterEnabledCacheManager(configuration);
      defineRecoveryCache(1);
      TestingUtil.blockUntilViewsReceived(60000, cache(0), cache(1));
      DummyTransaction t1_4 = beginAndSuspendTx(cache(1));
      prepareTransaction(t1_4);
      log.trace("Before main recovery call.");
      assertPrepared(4, t1_4);

      //now second call would only return 1 prepared tx as we only go over the network once
      assertPrepared(1, t1_4);

      commitTransaction(t1_4);
      assertPrepared(0, t1_4);

      inDoubtTransactions = rm(cache(0)).getInDoubtTransactions();
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
            return rm(cache(0)).getInDoubtTransactionInfo().size() == 2;
         }
      });
      inDoubtTransactions = rm(cache(0)).getInDoubtTransactions();
      assertEquals(2, inDoubtTransactions.size());
      assert inDoubtTransactions.contains(new SerializableXid(t1_2.getXid()));
      assert inDoubtTransactions.contains(new SerializableXid(t1_3.getXid()));

      t1_4.firstEnlistedResource().forget(t1_2.getXid());
      t1_4.firstEnlistedResource().forget(t1_3.getXid());
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return rm(cache(0)).getInDoubtTransactionInfo().size() == 0;
         }
      });
      assertEquals(0, rm(cache(0)).getInDoubtTransactionInfo().size());
   }

   protected void defineRecoveryCache(int cacheManagerIndex) {
   }

   protected String getRecoveryCacheName() {
      return RecoveryConfiguration.DEFAULT_RECOVERY_INFO_CACHE;
   }
}
