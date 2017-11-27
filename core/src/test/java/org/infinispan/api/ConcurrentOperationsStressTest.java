package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @author William Burns
 * @since 7.0
 */
@Test(groups = "stress", testName = "api.ConcurrentOperationsStressTest", timeOut = 15*60*1000)
public class ConcurrentOperationsStressTest extends ConcurrentOperationsTest {
   public ConcurrentOperationsStressTest() {
      super(CacheMode.DIST_SYNC, 3, 4, 300);
   }

   @Override
   public void testNoTimeout() throws Throwable {
      super.testNoTimeout();
   }

   @Override
   public void testNoTimeoutAndCorrectness() throws Throwable {
      super.testNoTimeoutAndCorrectness();
   }

   @Override
   public void testReplace() {
      super.testReplace();
   }
}
