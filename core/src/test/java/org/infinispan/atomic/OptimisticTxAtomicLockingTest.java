package org.infinispan.atomic;

import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "atomic.OptimisticTxAtomicLockingTest")
public class OptimisticTxAtomicLockingTest extends BaseAtomicMapLockingTest {

   public OptimisticTxAtomicLockingTest() {
      super(false, VersionedPrepareCommand.class);
   }
}
