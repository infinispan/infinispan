package org.infinispan.test.fwk;

import java.util.Set;

import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.ITestContext;

/**
 * DebuggingUnitTestNGListener is a slower version of UnitTestTestNGListener
 * containing some additional sanity checks of the tests themselves.
 * It will verify any clustered CacheManager created by the test was properly killed,
 * if not a message is output.
 *
 * NOTE: The test WILL NOT FAIL when not cleaning up, you'll have to check for these messages in logs.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
public class DebuggingUnitTestNGListener extends UnitTestTestNGListener {

   private static final Log log = LogFactory.getLog(DebuggingUnitTestNGListener.class);

   private static final Set<String> failedTestDescriptions = new ConcurrentHashSet<String>();

   @Override
   public void onFinish(ITestContext testCxt) {
      super.onFinish(testCxt);
      checkCleanedUp(testCxt);
   }

   private void checkCleanedUp(ITestContext testCxt) {
      CacheContainer cm = TestCacheManagerFactory.createClusteredCacheManager();
      try {
         cm.start();
         try {
            TestingUtil.blockUntilViewReceived(cm.getCache(), 1, 2000, true);
         } catch (RuntimeException re) {
            failedTestDescriptions.add(
                     "CacheManagers alive after test! - " + testCxt.getName() + " " + re.getMessage()
                     );
         }
      }
      finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public static void describeErrorsIfAny() {
      if (!failedTestDescriptions.isEmpty()) {
         log("~~~~~~~~~~~~~~~~~~~~~~~~~ TEST HEALTH INFO ~~~~~~~~~~~~~~~~~~~~~~~~~~");
         log("Some tests didn't properly shutdown the CacheManager:");
         for (String errorMsg : failedTestDescriptions) {
            log("\t" + errorMsg);
         }
         log("~~~~~~~~~~~~~~~~~~~~~~~~~ TEST HEALTH INFO ~~~~~~~~~~~~~~~~~~~~~~~~~~");
      }
   }

   private static void log(String s) {
      System.out.println(s);
      log.info(s);
   }

}
