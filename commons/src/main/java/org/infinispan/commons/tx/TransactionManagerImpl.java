package org.infinispan.commons.tx;


import java.util.UUID;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;


/**
 * A simple {@link TransactionManager} implementation.
 * <p>
 * It provides the basic to handle {@link Transaction}s and supports any {@link javax.transaction.xa.XAResource}.
 * <p>
 * Implementation notes: <ul> <li>The state is kept in memory only.</li> <li>Does not support recover.</li> <li>Does not
 * support multi-thread transactions. Although it is possible to execute the transactions in multiple threads, this
 * transaction manager does not wait for them to complete. It is the application responsibility to wait before invoking
 * {@link #commit()} or {@link #rollback()}</li> <li>The transaction should not block. It is no possible to {@link
 * #setTransactionTimeout(int)} and this transaction manager won't rollback the transaction if it takes too long.</li>
 * </ul>
 * <p>
 * If you need any of the requirements above, please consider use another implementation.
 * <p>
 * Also, it does not implement any 1-phase-commit optimization.
 *
 * @author Bela Ban
 * @author Pedro Ruivo
 * @since 9.1
 */
public abstract class TransactionManagerImpl implements TransactionManager {
   private static final Log log = LogFactory.getLog(TransactionManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private static ThreadLocal<Transaction> CURRENT_TRANSACTION = new ThreadLocal<>();
   protected final UUID transactionManagerId = UUID.randomUUID();

   public static void dissociateTransaction() {
      CURRENT_TRANSACTION.remove();
   }

   static void associateTransaction(Transaction tx) {
      CURRENT_TRANSACTION.set(tx);
   }

   @Override
   public Transaction getTransaction() {
      return CURRENT_TRANSACTION.get();
   }

   @Override
   public void begin() throws NotSupportedException, SystemException {
      Transaction currentTx = getTransaction();
      if (currentTx != null) {
         throw new NotSupportedException(
               Thread.currentThread() + " is already associated with a transaction (" + currentTx + ")");
      }
      associateTransaction(createTransaction());
   }

   @Override
   public void commit() throws RollbackException, HeuristicMixedException,
         HeuristicRollbackException, SecurityException,
         IllegalStateException, SystemException {
      getTransactionAndFailIfNone().commit();
      dissociateTransaction();
   }

   @Override
   public void rollback() throws IllegalStateException, SecurityException,
         SystemException {
      getTransactionAndFailIfNone().rollback();
      dissociateTransaction();
   }

   @Override
   public void setRollbackOnly() throws IllegalStateException, SystemException {
      getTransactionAndFailIfNone().setRollbackOnly();
   }

   @Override
   public int getStatus() throws SystemException {
      Transaction tx = getTransaction();
      return tx != null ? tx.getStatus() : Status.STATUS_NO_TRANSACTION;
   }

   @Override
   public void setTransactionTimeout(int seconds) throws SystemException {
      throw new SystemException("not supported");
   }

   @Override
   public Transaction suspend() throws SystemException {
      Transaction tx = getTransaction();
      dissociateTransaction();
      if (trace) {
         log.tracef("Suspending tx %s", tx);
      }
      return tx;
   }

   @Override
   public void resume(Transaction tx) throws InvalidTransactionException, IllegalStateException, SystemException {
      if (trace) {
         log.tracef("Resuming tx %s", tx);
      }
      associateTransaction(tx);
   }

   protected abstract Transaction createTransaction();

   /**
    * @return the current transaction (not null!)
    */
   private Transaction getTransactionAndFailIfNone() {
      Transaction transaction = CURRENT_TRANSACTION.get();
      if (transaction == null) {
         throw new IllegalStateException("no transaction associated with calling thread");
      }
      return transaction;
   }
}
