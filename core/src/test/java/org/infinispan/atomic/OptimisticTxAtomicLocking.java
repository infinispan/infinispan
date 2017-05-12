package org.infinispan.atomic;

import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "atomic.OptimisticTxAtomicLocking")
public class OptimisticTxAtomicLocking extends BaseAtomicMapLockingTest {

   public OptimisticTxAtomicLocking() {
      super(false, VersionedPrepareCommand.class);
   }
}
