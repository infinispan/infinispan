package org.infinispan.context.impl;

import org.infinispan.CacheException;
import org.infinispan.remoting.transport.Address;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * // TODO: Mircea: Document this!
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class AbstractTxInvocationContext extends AbstractInvocationContext implements TxInvocationContext {

   protected Set<Address> txParticipants = null;

   public boolean hasModifications() {
      return getModifications() != null && !getModifications().isEmpty();
   }

   public Set<Address> getTransactionParticipants() {
      return txParticipants == null ? Collections.EMPTY_SET : txParticipants;
   }

   public boolean isValidRunningTx() {
      return isValid(getRunningTransaction());
   }

   public void addTransactionParticipants(List<Address> addresses) {
      if (txParticipants == null) {
         txParticipants = new HashSet<Address>();
      }
      txParticipants.addAll(addresses);
   }


   /**
    * Return s true of tx's status is ACTIVE or PREPARING
    *
    * @param tx
    * @return true if the tx is active or preparing
    */
   public static boolean isValid(Transaction tx) {
      return isActive(tx) || isPreparing(tx);
   }

   /**
    * Returns true if transaction is ACTIVE, false otherwise
    */
   public static boolean isActive(Transaction tx) {
      if (tx == null) return false;
      int status;
      try {
         status = tx.getStatus();
         return status == Status.STATUS_ACTIVE;
      }
      catch (SystemException e) {
         return false;
      }
   }

   /**
    * Returns true if transaction is PREPARING, false otherwise
    */
   public static boolean isPreparing(Transaction tx) {
      if (tx == null) return false;
      int status;
      try {
         status = tx.getStatus();
         return status == Status.STATUS_PREPARING;
      }
      catch (SystemException e) {
         return false;
      }
   }

   /**
    * Tests whether the caller is in a valid transaction.  If not, will throw a CacheException.
    */
   public static void assertTransactionValid(TxInvocationContext ctx) {
      Transaction tx = ctx.getRunningTransaction();
      if (!isValid(tx)) try {
         throw new CacheException("Invalid transaction " + tx + ", status = " + (tx == null ? null : tx.getStatus()));
      }
      catch (SystemException e) {
         throw new CacheException("Exception trying to analyse status of transaction " + tx, e);
      }
   }

   @Override
   public AbstractTxInvocationContext clone() {
      AbstractTxInvocationContext dolly = (AbstractTxInvocationContext) super.clone();
      if (this.txParticipants != null) {
         dolly.txParticipants = new HashSet<Address>(txParticipants);
      }
      return dolly;
   }
}
