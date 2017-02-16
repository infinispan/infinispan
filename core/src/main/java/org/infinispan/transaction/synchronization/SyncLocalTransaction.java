package org.infinispan.transaction.synchronization;

import javax.transaction.Transaction;

import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * {@link LocalTransaction} implementation to be used with {@link SynchronizationAdapter}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class SyncLocalTransaction extends LocalTransaction {

   public SyncLocalTransaction(Transaction transaction, GlobalTransaction tx, boolean implicitTransaction,
                               int topologyId, long txCreationTime) {
      super(transaction, tx, implicitTransaction, topologyId, txCreationTime);
   }

   private boolean enlisted;

   @Override
   public boolean isEnlisted() {
      return enlisted;
   }

   public void setEnlisted(boolean enlisted) {
      this.enlisted = enlisted;
   }
}
