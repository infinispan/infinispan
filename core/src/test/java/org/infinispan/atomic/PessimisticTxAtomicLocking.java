package org.infinispan.atomic;

import org.infinispan.commands.tx.PrepareCommand;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "atomic.PessimisticTxAtomicLocking")
public class PessimisticTxAtomicLocking extends BaseAtomicMapLockingTest {

   public PessimisticTxAtomicLocking() {
      super(true, PrepareCommand.class);
   }
}
