package org.infinispan.api;

import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @author William Burns
 * @since 7.0
 */
@Test(groups = "stress", testName = "api.ConditionalOperationsConcurrentWriteSkewStressTest", timeOut = 15*60*1000)
public class ConditionalOperationsConcurrentWriteSkewStressTest extends ConditionalOperationsConcurrentStressTest {

   public ConditionalOperationsConcurrentWriteSkewStressTest() {
      transactional = true;
      writeSkewCheck = true;
   }
}
