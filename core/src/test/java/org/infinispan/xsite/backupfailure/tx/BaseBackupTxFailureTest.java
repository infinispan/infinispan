package org.infinispan.xsite.backupfailure.tx;

import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.infinispan.xsite.backupfailure.BaseBackupFailureTest;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite")
public abstract class BaseBackupTxFailureTest extends AbstractTwoSitesTest {

   private BaseBackupFailureTest.FailureInterceptor failureInterceptor;

   protected BaseBackupTxFailureTest() {
      isLonBackupTransactional = true;
      lonBackupFailurePolicy = BackupFailurePolicy.FAIL;
      use2Pc = true;
   }

   @Override
   protected void createSites() {
      super.createSites();
      failureInterceptor = new BaseBackupFailureTest.FailureInterceptor();
      backup("LON").getAdvancedCache().addInterceptor(failureInterceptor, 1);
   }

   @Test(groups = "unstable_xsite")
   public void testPrepareFailure() {
      try {
         cache("LON", 0).put("k","v");
         fail("This should have thrown an exception");
      } catch (Exception e) {
      }
      assertNull(cache("LON",0).get("k"));
      assertNull(cache("LON",1).get("k"));
      assertNull(backup("LON").get("k"));
      assertEquals(0, txTable(cache("LON", 0)).getLocalTransactions().size());
      assertEquals(0, txTable(cache("LON", 1)).getLocalTransactions().size());
      assertEquals(0, txTable(backup("LON")).getLocalTransactions().size());
   }
}
