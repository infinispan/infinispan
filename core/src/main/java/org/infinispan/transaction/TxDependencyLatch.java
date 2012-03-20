package org.infinispan.transaction;

import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.concurrent.CountDownLatch;

import static org.infinispan.util.Util.prettyPrintGlobalTransaction;

/**
 * This latch represents the dependencies between the transactions. A transaction has a set of this latch and
 * it will wait on each one. The latch is released when the transaction finishes (ie, the modification are applied or
 * it is rollbacked)
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TxDependencyLatch extends CountDownLatch {
   private String globalTransaction;

   public TxDependencyLatch(GlobalTransaction globalTransaction) {
      super(1);
      this.globalTransaction = prettyPrintGlobalTransaction(globalTransaction);
   }

   @Override
   public String toString() {
      return "TxDependencyLatch{" +
            "globalTransaction='" + globalTransaction + '\'' +
            '}';
   }
}
