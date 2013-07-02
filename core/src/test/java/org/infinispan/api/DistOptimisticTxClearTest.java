package org.infinispan.api;

import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "api.DistOptimisticTxClearTest")
public class DistOptimisticTxClearTest extends BaseDistClearTest {

   public DistOptimisticTxClearTest() {
      super(true, true);
   }
}
