package org.infinispan.xsite.backupfailure.tx;

import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import org.infinispan.xsite.AbstractTwoSitesTest;
import org.infinispan.xsite.backupfailure.BaseBackupFailureTest;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite")
public abstract class BaseLocalClusterTxFailureTest extends AbstractTwoSitesTest {

   private BaseBackupFailureTest.FailureInterceptor failureInterceptor;

   @Override
   protected void createSites() {
      super.createSites();
      failureInterceptor = new BaseBackupFailureTest.FailureInterceptor();
      cache("LON", 1).getAdvancedCache().addInterceptor(failureInterceptor, 1);
   }

   public void testPrepareFailure() {
      try {
         cache("LON", 0).put("k","v");
         fail("This should have thrown an exception");
      } catch (Exception e) {
      }
      assertNull(cache("LON",0).get("k"));
      assertNull(cache("LON",1).get("k"));
      assertNull(backup("LON").get("k"));
   }
}
