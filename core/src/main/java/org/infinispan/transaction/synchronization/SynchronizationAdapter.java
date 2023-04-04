package org.infinispan.transaction.synchronization;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import jakarta.transaction.Synchronization;

import org.infinispan.commons.tx.AsyncSynchronization;
import org.infinispan.transaction.impl.AbstractEnlistmentAdapter;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * {@link Synchronization} implementation for integrating with the TM. See <a href="https://issues.jboss.org/browse/ISPN-888">ISPN-888</a>
 * for more information on this.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class SynchronizationAdapter extends AbstractEnlistmentAdapter implements Synchronization, AsyncSynchronization {
   private final LocalTransaction localTransaction;
   private final TransactionTable txTable;

   public SynchronizationAdapter(LocalTransaction localTransaction, TransactionTable txTable) {
      super(localTransaction);
      this.localTransaction = localTransaction;
      this.txTable = txTable;
   }

   @Override
   public void beforeCompletion() {
      CompletionStages.join(txTable.beforeCompletion(localTransaction));
   }

   @Override
   public void afterCompletion(int status) {
      CompletionStages.join(txTable.afterCompletion(localTransaction, status));
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

      return Objects.equals(localTransaction, that.localTransaction);
   }

   @Override
   public CompletionStage<Void> asyncBeforeCompletion() {
      return txTable.beforeCompletion(localTransaction)
            .thenApply(CompletableFutures.toNullFunction());
   }

   @Override
   public CompletionStage<Void> asyncAfterCompletion(int status) {
      return txTable.afterCompletion(localTransaction, status);
   }
}
