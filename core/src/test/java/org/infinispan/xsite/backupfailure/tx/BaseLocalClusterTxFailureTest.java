package org.infinispan.xsite.backupfailure.tx;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.infinispan.xsite.backupfailure.BaseBackupFailureTest;
import org.testng.annotations.Test;

import jakarta.transaction.RollbackException;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite")
public abstract class BaseLocalClusterTxFailureTest extends AbstractTwoSitesTest {

   private BaseBackupFailureTest.FailureInterceptor failureInterceptor;

   @Override
   protected void createSites() {
      super.createSites();
      failureInterceptor = new BaseBackupFailureTest.FailureInterceptor();
      extractInterceptorChain(cache(LON, 1)).addInterceptor(failureInterceptor, 1);
   }

   public void testPrepareFailure() {
      failureInterceptor.enable();
      try {
         Exceptions.expectException(CacheException.class, RollbackException.class, () -> cache(LON, 0).put("k", "v"));
      } finally {
         failureInterceptor.disable();
      }
      assertNull(cache(LON,0).get("k"));
      assertNull(cache(LON,1).get("k"));
      assertNull(backup(LON).get("k"));
   }
}
