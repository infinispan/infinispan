package org.infinispan.transaction.tm;


import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

/**
 * A {@link UserTransaction} implementation that uses {@link EmbeddedTransactionManager}.
 * <p>
 * This implementation does not support transaction timeout and it does not cancel long running transactions.
 * <p>
 * See {@link EmbeddedBaseTransactionManager} for more details about its implementation.
 *
 * @author Bela Ban
 * @author Pedro Ruivo
 * @see EmbeddedBaseTransactionManager
 * @since 9.0
 */
public class EmbeddedUserTransaction implements UserTransaction {
   private final EmbeddedTransactionManager tm;

   EmbeddedUserTransaction(EmbeddedTransactionManager tm) {
      this.tm = tm;
   }

   @Override
   public void begin() throws NotSupportedException, SystemException {
      tm.begin();
   }

   @Override
   public void commit()
         throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException,
         SystemException {
      tm.commit();
   }

   @Override
   public void rollback() throws IllegalStateException, SystemException {
      tm.rollback();
   }

   @Override
   public void setRollbackOnly() throws IllegalStateException, SystemException {
      tm.setRollbackOnly();
   }

   @Override
   public int getStatus() throws SystemException {
      return tm.getStatus();
   }

   @Override
   public void setTransactionTimeout(int seconds) throws SystemException {
      throw new SystemException("not supported");
   }

}
