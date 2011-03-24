package org.infinispan.transaction.xa;

import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.recovery.XidAware;

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

   public LocalXaTransaction(Transaction transaction, GlobalTransaction tx) {
      super(transaction, tx);
   }

   public void setXid(Xid xid) {
      this.xid  = xid;
      if (tx instanceof XidAware) {
         ((XidAware) tx).setXid(xid);
      }
   }

   public Xid getXid() {
      return xid;
   }

   /**
    * As per the JTA spec, XAResource.start is called on enlistment. That method also sets the xid for this local
    * transaction.
    */
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
