package org.infinispan.transaction.xa;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.xa.recovery.RecoverableTransactionIdentifier;

import javax.transaction.Transaction;
import javax.transaction.xa.Xid;

/**
 * {@link LocalTransaction} implementation to be used with {@link TransactionXaAdapter}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class LocalXaTransaction extends LocalTransaction {

   private Xid xid;

   public LocalXaTransaction(Transaction transaction, GlobalTransaction tx,
         boolean implicitTransaction, int topologyId, Equivalence<Object> keyEquivalence) {
      super(transaction, tx, implicitTransaction, topologyId, keyEquivalence);
   }

   public void setXid(Xid xid) {
      this.xid  = xid;
      if (tx instanceof RecoverableTransactionIdentifier) {
         ((RecoverableTransactionIdentifier) tx).setXid(xid);
      }
   }

   public Xid getXid() {
      return xid;
   }

   /**
    * As per the JTA spec, XAResource.start is called on enlistment. That method also sets the xid for this local
    * transaction.
    */
   @Override
   public boolean isEnlisted() {
      return xid != null;
   }

   @Override
   public String toString() {
      return "LocalXaTransaction{" +
            "xid=" + xid +
            "} " + super.toString();
   }
}
