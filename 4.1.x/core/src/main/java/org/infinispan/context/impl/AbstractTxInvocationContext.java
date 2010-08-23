package org.infinispan.context.impl;

import org.infinispan.CacheException;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Support class for {@link org.infinispan.context.impl.TxInvocationContext}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class AbstractTxInvocationContext extends AbstractInvocationContext implements TxInvocationContext {

   protected Set<Object> affectedKeys = null;

   public boolean hasModifications() {
      return getModifications() != null && !getModifications().isEmpty();
   }

   public Set<Object> getAffectedKeys() {
      return affectedKeys == null ? Collections.EMPTY_SET : affectedKeys;
   }

   public boolean isValidRunningTx() {
      return isValid(getRunningTransaction());
   }

   public void addAffectedKeys(Object... keys) {
      if (affectedKeys == null) {
         affectedKeys = new HashSet<Object>();
      }
      affectedKeys.addAll(Arrays.asList(keys));
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
      if (this.affectedKeys != null) {
         dolly.affectedKeys = new HashSet<Object>(affectedKeys);
      }
      return dolly;
   }
}
