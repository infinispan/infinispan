package org.infinispan.tx.recovery.admin;

import static org.infinispan.tx.recovery.RecoveryTestUtil.prepareTransaction;
import static org.testng.Assert.assertEquals;

import java.util.List;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

/**
 * Tests the following scenario: the transaction originator fails and it also part of the transactions.
 *
 * @author Mircea Markus
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.recovery.admin.OriginatorAndOwnerFailureTest")
@CleanupAfterMethod
public class OriginatorAndOwnerFailureTest extends AbstractRecoveryTest {

   private Object key;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configuration = defaultRecoveryConfig();
      assert configuration.build().transaction().transactionMode().isTransactional();
      createCluster(ControlledConsistentHashFactory.SCI.INSTANCE, configuration, 3);
      waitForClusterToForm();

      key = getKey();

      tm(2).begin();
      cache(2).put(this.key, "newValue");
      EmbeddedTransaction tx = (EmbeddedTransaction) tm(2).suspend();
      prepareTransaction(tx);

      killMember(2);

      assert !recoveryOps(0).showInDoubtTransactions().isEmpty();
      assert !recoveryOps(1).showInDoubtTransactions().isEmpty();
   }

   protected Object getKey() {
      return new MagicKey(cache(2));
   }

   public void recoveryInvokedOnNonTxParticipantTest() {
      runTest(false);
   }

   public void recoveryInvokedOnTxParticipantTest() {
      runTest(true);
   }

   private void runTest(boolean txParticipant) {
      int index = getTxParticipant(txParticipant);
      runTest(index);
   }

   protected void runTest(int index) {

      assert cache(0).getCacheConfiguration().transaction().transactionMode().isTransactional();

      List<Long> internalIds = getInternalIds(recoveryOps(index).showInDoubtTransactions());
      assertEquals(internalIds.size(), 1);

      assertEquals(cache(0).get(key), null);
      assertEquals(cache(1).get(key), null);

      log.trace("About to force commit!");
      isSuccess(recoveryOps(index).forceCommit(internalIds.get(0)));

      assertEquals(cache(0).get(key), "newValue");
      assertEquals(cache(1).get(key), "newValue");

      assertCleanup(0);
      assertCleanup(1);
   }
}
