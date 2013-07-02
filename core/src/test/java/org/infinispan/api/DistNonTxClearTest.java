package org.infinispan.api;

import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "api.DistNonTxClearTest")
public class DistNonTxClearTest extends BaseDistClearTest {

   public DistNonTxClearTest() {
      super(false, false);
   }
}
