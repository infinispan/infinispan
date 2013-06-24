package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "api.ReplicatedOptimisticRepeatableReadIsolationTest")
public class ReplicatedOptimisticRepeatableReadIsolationTest extends AbstractRepeatableReadIsolationTest {

   public ReplicatedOptimisticRepeatableReadIsolationTest() {
      super(CacheMode.REPL_SYNC, LockingMode.OPTIMISTIC);
   }
}
