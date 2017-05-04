package org.infinispan.tx.recovery;

import static org.infinispan.tx.recovery.RecoveryTestUtil.beginAndSuspendTx;
import static org.infinispan.tx.recovery.RecoveryTestUtil.prepareTransaction;
import static org.testng.AssertJUnit.assertEquals;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.recovery.InDoubtXidReturnedOnceTest")
public class InDoubtXidReturnedOnceTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      configuration
         .locking()
            .useLockStriping(false)
         .transaction()
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .useSynchronization(false)
            .recovery()
               .enable()
         .clustering()
            .stateTransfer()
               .fetchInMemoryState(false)
            .hash()
               .numOwners(3);

      createCluster(configuration, 4);
      waitForClusterToForm();
   }

   public void testXidReturnedOnlyOnce() throws Throwable {
      EmbeddedTransaction dummyTransaction1 = beginAndSuspendTx(this.cache(3));
      prepareTransaction(dummyTransaction1);
      manager(3).stop();
      TestingUtil.blockUntilViewsReceived(60000, false, cache(0), cache(1), cache(2));
      TestingUtil.waitForNoRebalance(cache(0), cache(1), cache(2));


      EmbeddedTransaction dummyTransaction = beginAndSuspendTx(this.cache(0));
      Xid[] recover = dummyTransaction.firstEnlistedResource().recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
      assertEquals(recover.length,1);
      assertEquals(dummyTransaction1.getXid(), recover[0]);

   }
}
