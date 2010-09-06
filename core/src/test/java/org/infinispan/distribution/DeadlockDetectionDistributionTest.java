package org.infinispan.distribution;

import org.infinispan.config.Configuration;
import org.infinispan.tx.ReplDeadlockDetectionTest;
import static org.testng.Assert.fail;
import org.testng.annotations.Test;

/**
 * Test deadlock detection when cache is configured for distribution.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional",  testName = "distribution.DeadlockDetectionDistributionTest")
public class DeadlockDetectionDistributionTest extends ReplDeadlockDetectionTest {

//   public DeadlockDetectionDistributionTest() {
//      cacheMode = Configuration.CacheMode.DIST_SYNC;
//   }
//
//
//   public void testDeadlockDetectedTwoTransactions() throws Exception {
//      fail("This test should be updated to make sure tx replicate on opposite nodes");
//   }
//
//
//   //following methods are overridden as TestNG will otherwise run them even if I mark the class as enabled = false
//
//   @Override
//   public void testExpectedInnerStructure() {
//      throw new IllegalStateException("TODO - please implement me!!!"); //todo implement!!!
//   }
//
//   @Override
//   public void testDeadlockDetectedOneTx() throws Exception {
//      throw new IllegalStateException("TODO - please implement me!!!"); //todo implement!!!
//   }
//
//   @Override
//   public void testLockReleasedWhileTryingToAcquire() throws Exception {
//      throw new IllegalStateException("TODO - please implement me!!!"); //todo implement!!!
//   }
}
