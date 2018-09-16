package org.infinispan.transaction.tm;


import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

/**
 * @author bela
 * @since 4.0
 * @deprecated use {@link EmbeddedUserTransaction}
 */
@Deprecated
public class DummyUserTransaction implements UserTransaction, java.io.Serializable {
   DummyTransactionManager tm;
   private static final long serialVersionUID = -6568400755677046127L;

   public DummyUserTransaction(DummyTransactionManager tm) {
      this.tm = tm;
   }


   /**
    * Starts a new transaction, and associate it with the calling thread.
    *
    * @throws NotSupportedException If the calling thread is already associated with a transaction, and nested
    *                               transactions are not supported.
    * @throws SystemException       If the transaction service fails in an unexpected way.
    */
   @Override
   public void begin() throws NotSupportedException, SystemException {
      tm.begin();
   }

   /**
    * Attempt to commit this transaction.
    *
    * @throws RollbackException          If the transaction was marked for rollback only, the transaction is rolled back
    *                                    and this exception is thrown.
    * @throws SystemException            If the transaction service fails in an unexpected way.
    * @throws HeuristicMixedException    If a heuristic decision was made and some some parts of the transaction have
    *                                    been committed while other parts have been rolled back.
    * @throws HeuristicRollbackException If a heuristic decision to roll back the transaction was made.
    * @throws SecurityException          If the caller is not allowed to commit this transaction.
    */
   @Override
   public void commit()
         throws RollbackException, HeuristicMixedException,
                HeuristicRollbackException, SecurityException, SystemException {

      tm.commit();
   }

   /**
    * Rolls back this transaction.
    *
    * @throws IllegalStateException If the transaction is in a state where it cannot be rolled back. This could be
    *                               because the transaction is no longer active, or because it is in the {@link
    *                               Status#STATUS_PREPARED prepared state}.
    * @throws SystemException       If the transaction service fails in an unexpected way.
    */
   @Override
   public void rollback() throws IllegalStateException, SystemException {
      tm.rollback();
   }

   /**
    * Mark the transaction so that the only possible outcome is a rollback.
    *
    * @throws IllegalStateException If the transaction is not in an active state.
    * @throws SystemException       If the transaction service fails in an unexpected way.
    */
   @Override
   public void setRollbackOnly() throws IllegalStateException, SystemException {
      tm.setRollbackOnly();
   }

   /**
    * Get the status of the transaction.
    *
    * @return The status of the transaction. This is one of the {@link Status} constants.
    * @throws SystemException If the transaction service fails in an unexpected way.
    */
   @Override
   public int getStatus() throws SystemException {
      return tm.getStatus();
   }

   /**
    * Change the transaction timeout for transactions started by the calling thread with the {@link #begin()} method.
    *
    * @param seconds The new timeout value, in seconds. If this parameter is <code>0</code>, the timeout value is reset
    *                to the default value.
    * @throws SystemException If the transaction service fails in an unexpected way.
    */
   @Override
   public void setTransactionTimeout(int seconds) throws SystemException {
      throw new SystemException("not supported");
   }

}
