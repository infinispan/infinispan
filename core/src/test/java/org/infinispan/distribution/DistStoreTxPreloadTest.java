package org.infinispan.distribution;

import org.testng.annotations.Test;

/**
 * Test preloading with transactional distributed caches.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "distribution.DistStoreTxPreloadTest")
public class DistStoreTxPreloadTest extends DistStorePreloadTest {

   public DistStoreTxPreloadTest() {
      tx = true;
   }

}
