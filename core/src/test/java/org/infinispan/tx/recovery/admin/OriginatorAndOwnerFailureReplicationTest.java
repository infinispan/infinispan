package org.infinispan.tx.recovery.admin;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.recovery.admin.OriginatorAndOwnerFailureReplicationTest")
@CleanupAfterMethod
public class OriginatorAndOwnerFailureReplicationTest extends OriginatorAndOwnerFailureTest {

   @Override
   protected ConfigurationBuilder defaultRecoveryConfig() {
      ConfigurationBuilder configuration = super.defaultRecoveryConfig();
      configuration.clustering().cacheMode(CacheMode.REPL_SYNC);
      return configuration;
   }

   @Override
   protected Object getKey() {
      return "aKey";
   }

   @Test
   public void recoveryInvokedOnNonTxParticipantTest() {
      //all nodes are tx participants in replicated caches so this test makes no sense
   }

   @Override
   public void recoveryInvokedOnTxParticipantTest() {
      runTest(0);
   }
}
