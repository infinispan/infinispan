package org.infinispan.transaction.synchronization;

import java.util.concurrent.Executor;

import javax.transaction.Synchronization;

import org.infinispan.transaction.impl.AbstractEnlistmentAdapter;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * {@link Synchronization} implementation for integrating with the TM.
 * See <a href="https://issues.jboss.org/browse/ISPN-888">ISPN-888</a> for more information on this.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class SynchronizationAdapter extends AbstractEnlistmentAdapter implements Synchronization {
   private final LocalTransaction localTransaction;
   private final TransactionTable txTable;
   private final Executor executor;

   public SynchronizationAdapter(LocalTransaction localTransaction, TransactionTable txTable, Executor executor) {
      super(localTransaction);
      this.localTransaction = localTransaction;
      this.txTable = txTable;
      this.executor = executor;
   }

   @Override
   public void beforeCompletion() {
      CompletionStages.join(CompletableFutures.completedNull()
            .thenComposeAsync(ignore -> txTable.beforeCompletion(localTransaction), executor));
   }

   @Override
   public void afterCompletion(int status) {
      CompletionStages.join(CompletableFutures.completedNull()
            .thenComposeAsync(ignore -> txTable.afterCompletion(localTransaction, status), executor));
   }

   @Override
   public String toString() {
      return "SynchronizationAdapter{" +
            "localTransaction=" + localTransaction +
            "} " + super.toString();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SynchronizationAdapter that = (SynchronizationAdapter) o;

      if (localTransaction != null ? !localTransaction.equals(that.localTransaction) : that.localTransaction != null)
         return false;

      return true;
   }
}
