package org.infinispan.xsite.backupfailure.tx;

import static org.testng.AssertJUnit.fail;

import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.infinispan.xsite.backupfailure.BaseBackupFailureTest;
import org.testng.annotations.Test;

@Test(groups = "xsite", testName = "xsite.backupfailure.tx.SinglePhaseCommitFailureTest")
public class SinglePhaseCommitFailureTest extends AbstractTwoSitesTest {

   private BaseBackupFailureTest.FailureInterceptor failureInterceptor;

   public SinglePhaseCommitFailureTest() {
      use2Pc = false;
      lonBackupFailurePolicy = BackupFailurePolicy.FAIL;
   }

   @Override
   protected void createSites() {
      super.createSites();
      failureInterceptor = new BaseBackupFailureTest.FailureInterceptor();
      backup("LON").getAdvancedCache().addInterceptor(failureInterceptor, 1);
   }

   public void testPrepareFailure() {
      try {
         cache("LON", 0).put("k", "v");
         fail("This should have thrown an exception");
      } catch (Exception e) {
      }
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      dcc.transaction().useSynchronization(false);//this makes the TM throw an exception if the 2nd phase fails
      return dcc;
   }
}
