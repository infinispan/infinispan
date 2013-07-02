package org.infinispan.api;

import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "api.ConditionalOperationsConcurrentPessimisticTest")
public class ConditionalOperationsConcurrentPessimisticTest extends ConditionalOperationsConcurrentTest {

   public ConditionalOperationsConcurrentPessimisticTest() {
      transactional = true;
      lockingMode = LockingMode.PESSIMISTIC;
   }
}
