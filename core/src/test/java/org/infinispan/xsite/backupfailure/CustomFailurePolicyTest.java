package org.infinispan.xsite.backupfailure;

import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.xsite.CountingCustomFailurePolicy;
import org.testng.annotations.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.backupfailure.CustomFailurePolicyTest")
public class CustomFailurePolicyTest extends NonTxBackupFailureTest{

   public CustomFailurePolicyTest() {
      lonBackupFailurePolicy = BackupFailurePolicy.CUSTOM;
      lonCustomFailurePolicyClass = CountingCustomFailurePolicy.class.getName();
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   @Override
   public void testPutFailure() {
      assertFalse(CountingCustomFailurePolicy.PUT_INVOKED);
      super.testPutFailure();
      assertTrue(CountingCustomFailurePolicy.PUT_INVOKED);
   }

   @Override
   public void testRemoveFailure() {
      assertFalse(CountingCustomFailurePolicy.REMOVE_INVOKED);
      super.testRemoveFailure();
      assertTrue(CountingCustomFailurePolicy.REMOVE_INVOKED);
   }

   @Override
   public void testReplaceFailure() {
      assertFalse(CountingCustomFailurePolicy.REPLACE_INVOKED);
      super.testReplaceFailure();
      assertTrue(CountingCustomFailurePolicy.REPLACE_INVOKED);
   }

   @Override
   public void testClearFailure() {
      assertFalse(CountingCustomFailurePolicy.CLEAR_INVOKED);
      super.testClearFailure();
      assertTrue(CountingCustomFailurePolicy.CLEAR_INVOKED);
   }

   @Override
   public void testPutMapFailure() {
      assertFalse(CountingCustomFailurePolicy.PUT_ALL_INVOKED);
      super.testPutMapFailure();
      assertTrue(CountingCustomFailurePolicy.PUT_ALL_INVOKED);
   }
}
