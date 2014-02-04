package org.infinispan.xsite.backupfailure.tx;

import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.xsite.CountingCustomFailurePolicy;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.backupfailure.tx.CustomFailurePolicyTxTest")
public class CustomFailurePolicyTxTest extends BaseBackupTxFailureTest {

   public CustomFailurePolicyTxTest() {
      lonBackupFailurePolicy = BackupFailurePolicy.CUSTOM;
      lonCustomFailurePolicyClass = CountingCustomFailurePolicy.class.getName();
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   @Test(groups = "unstable_xsite")
   @Override
   public void testPrepareFailure() {
      assertFalse(CountingCustomFailurePolicy.PREPARE_INVOKED);
      super.testPrepareFailure();
      assertTrue(CountingCustomFailurePolicy.PREPARE_INVOKED);
   }
}
