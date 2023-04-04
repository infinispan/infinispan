package org.infinispan.transaction.xa;

import jakarta.transaction.Transaction;

import org.infinispan.commons.tx.XidImpl;
import org.infinispan.transaction.impl.LocalTransaction;

/**
 * {@link LocalTransaction} implementation to be used with {@link TransactionXaAdapter}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class LocalXaTransaction extends LocalTransaction {

   private XidImpl xid;

   public LocalXaTransaction(Transaction transaction, GlobalTransaction tx, boolean implicitTransaction, int topologyId,
                             long txCreationTime) {
      super(transaction, tx, implicitTransaction, topologyId, txCreationTime);
   }

   public void setXid(XidImpl xid) {
      this.xid  = xid;
      tx.setXid(xid);
   }

   public XidImpl getXid() {
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
