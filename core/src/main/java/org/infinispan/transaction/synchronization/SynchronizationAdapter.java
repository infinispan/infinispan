package org.infinispan.transaction.synchronization;

import javax.transaction.Synchronization;

import org.infinispan.transaction.impl.AbstractEnlistmentAdapter;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;

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

   public SynchronizationAdapter(LocalTransaction localTransaction, TransactionTable txTable) {
      super(localTransaction);
      this.localTransaction = localTransaction;
      this.txTable = txTable;
   }

   @Override
   public void beforeCompletion() {
      txTable.beforeCompletion(localTransaction);
   }

   @Override
   public void afterCompletion(int status) {
      txTable.afterCompletion(localTransaction, status);
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
