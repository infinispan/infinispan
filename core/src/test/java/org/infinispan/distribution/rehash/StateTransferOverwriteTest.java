package org.infinispan.distribution.rehash;

import java.util.concurrent.Callable;
import java.util.function.Predicate;

import javax.transaction.TransactionManager;

import org.infinispan.commands.VisitableCommand;
import org.testng.annotations.Test;

/**
 * Test that ensures that state transfer values aren't overridden with a non tx without L1 enabled.
 *
 * @author William Burns
 * @since 6.0
 */
@Test(groups = "functional", testName = "distribution.rehash.StateTransferOverwriteTest")
public class StateTransferOverwriteTest extends BaseTxStateTransferOverwriteTest {
   @Override
   public Object[] factory() {
      return new Object[] {
         new StateTransferOverwriteTest().l1(false),
         new StateTransferOverwriteTest().l1(true),
      };
   }

   public StateTransferOverwriteTest() {
      transactional = false;
   }

   @Override
   protected Predicate<VisitableCommand> isExpectedCommand(TestWriteOperation op) {
      return op.getCommandClass()::isInstance;
   }

   @Override
   protected Callable<Object> runWithTx(final TransactionManager tm, final Callable<?> callable) {
      return (Callable<Object>)callable;
   }
}
