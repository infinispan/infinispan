package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "api.ConditionalOperationsConcurrentPessimisticTest")
public class ConditionalOperationsConcurrentPessimisticTest extends ConditionalOperationsConcurrentTest {

   public ConditionalOperationsConcurrentPessimisticTest() {
      cacheMode = CacheMode.DIST_SYNC;
      transactional = true;
      lockingMode = LockingMode.PESSIMISTIC;
   }
}
