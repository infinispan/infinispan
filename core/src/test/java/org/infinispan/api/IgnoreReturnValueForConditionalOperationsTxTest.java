package org.infinispan.api;

import org.testng.annotations.Test;

/**
 * https://issues.jboss.org/browse/ISPN-3141
 */
@Test (groups = "functional", testName = "api.IgnoreReturnValueForConditionalOperationsTxTest")
public class IgnoreReturnValueForConditionalOperationsTxTest extends IgnoreReturnValueForConditionalOperationsTest {
   public IgnoreReturnValueForConditionalOperationsTxTest() {
      transactional = true;
   }
}
