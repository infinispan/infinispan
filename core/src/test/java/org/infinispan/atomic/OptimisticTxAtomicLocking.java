package org.infinispan.atomic;

import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "atomic.OptimisticTxAtomicLocking")
public class OptimisticTxAtomicLocking extends BaseAtomicMapLockingTest {

   public OptimisticTxAtomicLocking() {
      super(false);
   }
}
