package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "api.DistributedOptimisticRepeatableReadIsolationTest")
public class DistributedOptimisticRepeatableReadIsolationTest extends AbstractRepeatableReadIsolationTest {

   public DistributedOptimisticRepeatableReadIsolationTest() {
      super(CacheMode.DIST_SYNC, LockingMode.OPTIMISTIC);
   }
}
