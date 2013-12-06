package org.infinispan.distribution.rehash;

import org.infinispan.commands.VisitableCommand;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.concurrent.Callable;

/**
 * Test that ensures that state transfer values aren't overridden with a non tx without L1 enabled.
 *
 * @author William Burns
 * @since 6.0
 */
@Test(groups = "functional", testName = "distribution.rehash.StateTransferOverwriteTest")
public class StateTransferOverwriteTest extends BaseTxStateTransferOverwriteTest {
   public StateTransferOverwriteTest() {
      super();
      tx = false;
   }

   @Override
   protected Class<? extends VisitableCommand> getVisitableCommand(TestWriteOperation op) {
      return op.getCommandClass();
   }

   @Override
   protected Callable<Object> runWithTx(final TransactionManager tm, final Callable<? extends Object> callable) {
      return (Callable<Object>)callable;
   }
}
