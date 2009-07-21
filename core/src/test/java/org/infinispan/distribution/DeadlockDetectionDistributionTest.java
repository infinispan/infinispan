package org.infinispan.distribution;

import org.infinispan.config.Configuration;
import org.infinispan.tx.DeadlockDetectionTest;
import static org.testng.Assert.fail;
import org.testng.annotations.Test;

/**
 * Test deadlock detection when cache is configured for distribution.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", enabled = false, testName = "tx.DeadlockDetectionDistributionTest")
public class DeadlockDetectionDistributionTest extends DeadlockDetectionTest {

   public DeadlockDetectionDistributionTest() {
      cacheMode = Configuration.CacheMode.DIST_SYNC;
   }


   public void testDeadlockDetectedTwoTransactions() throws Exception {
      fail("This test should be updated to make sure tx replicate on opposite nodes");
   }
}
