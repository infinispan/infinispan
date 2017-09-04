package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @author William Burns
 * @since 7.0
 */
@Test (groups = "stress", testName = "api.ConditionalOperationsConcurrentPessimisticStressTest")
public class ConditionalOperationsConcurrentPessimisticStressTest extends ConditionalOperationsConcurrentStressTest {

   public ConditionalOperationsConcurrentPessimisticStressTest() {
      cacheMode = CacheMode.DIST_SYNC;
      transactional = true;
      lockingMode = LockingMode.PESSIMISTIC;
   }
}
