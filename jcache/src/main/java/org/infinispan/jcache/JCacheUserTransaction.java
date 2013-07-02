package org.infinispan.jcache;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

/**
 * A {@link UserTransaction} facade for JCache's requirement to provide
 * access to a user transaction implementation. It delegates to the
 * transaction manager.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class JCacheUserTransaction implements UserTransaction {

   private final TransactionManager tm;

   public JCacheUserTransaction(TransactionManager tm) {
      this.tm = tm;
   }

   @Override
   public void begin() throws NotSupportedException, SystemException {
      tm.begin();
   }

   @Override
   public void commit() throws RollbackException, HeuristicMixedException,
         HeuristicRollbackException, SecurityException,
         IllegalStateException, SystemException {
      tm.commit();
   }

   @Override
   public void rollback() throws IllegalStateException, SecurityException, SystemException {
      tm.rollback();
   }

   @Override
   public void setRollbackOnly() throws IllegalStateException, SystemException {
      tm.setRollbackOnly();
   }

   @Override
   public int getStatus() throws SystemException {
      if (tm == null)
         return Status.STATUS_NO_TRANSACTION;

      return tm.getStatus();
   }

   @Override
   public void setTransactionTimeout(int seconds) throws SystemException {
      tm.setTransactionTimeout(seconds);
   }

}
