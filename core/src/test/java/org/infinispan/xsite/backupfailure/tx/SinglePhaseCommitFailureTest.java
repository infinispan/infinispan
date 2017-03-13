package org.infinispan.xsite.backupfailure.tx;

import static org.infinispan.test.Exceptions.expectException;

import org.infinispan.commons.CacheException;
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
      backup("LON").getAdvancedCache().getAsyncInterceptorChain().addInterceptor(failureInterceptor, 1);
   }

   public void testPrepareFailure() {
      failureInterceptor.enable();
      try {
         expectException(CacheException.class, () -> cache("LON", 0).put("k", "v"));
      } finally {
         failureInterceptor.disable();
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
