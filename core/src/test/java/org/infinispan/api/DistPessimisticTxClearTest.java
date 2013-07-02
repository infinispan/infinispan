package org.infinispan.api;

import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "api.DistPessimisticTxClearTest")
public class DistPessimisticTxClearTest extends BaseDistClearTest {

   public DistPessimisticTxClearTest() {
      super(true, false);
   }
}
