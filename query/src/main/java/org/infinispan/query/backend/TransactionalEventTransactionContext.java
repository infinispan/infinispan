package org.infinispan.query.backend;

import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;

import org.hibernate.search.backend.TransactionContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This class implements the {@link org.hibernate.search.backend.TransactionContext} interface.  It retrieves
 * transaction context information from the {@link javax.transaction.TransactionManager} that gets passed in as a
 * parameter upon instantiation.
 * <p/>
 * It is used by the {@link QueryInterceptor} to pass transaction information to a Hibernate Search {@link
 * org.hibernate.search.backend.Work} object.
 * <p/>
 * <p/>
 *
 * @author Navin Surtani
 * @see QueryInterceptor
 */
public class TransactionalEventTransactionContext implements TransactionContext {

   private final TransactionManager transactionManager;
   private final TransactionSynchronizationRegistry transactionSynchronizationRegistry;
   private static final Log log = LogFactory.getLog(TransactionalEventTransactionContext.class);

   /**
    * Creates a new instance of NodeModifiedTransactionContext.
    * <p/>
    *
    * @param transactionManager a NodeModifiedEvent to wrap.  Should not be null.
    * @param transactionSynchronizationRegistry
    * @throws NullPointerException if event is null.
    */
   public TransactionalEventTransactionContext(TransactionManager transactionManager, final TransactionSynchronizationRegistry transactionSynchronizationRegistry) {
      this.transactionManager = transactionManager;
      this.transactionSynchronizationRegistry = transactionSynchronizationRegistry;
   }

   /**
    * Returns a boolean value whether or not a transaction is in progress (JTA transaction and in this case *not* an
    * org.hibernate transaction).
    *
    * @return true if a transaction is in progress, false otherwise.
    */
   @Override
   public boolean isTransactionInProgress() {
      if (transactionManager == null) return false;

      Transaction transaction = null;
      try {
         transaction = transactionManager.getTransaction();
      }
      catch (SystemException e) {
         log.debug("Caught SystemException in TransactionalEventTransactionContext", e);
      }
      return (transaction != null);
   }

   /**
    * Returns a JTA transaction.
    *
    * @return a JTA transaction if one is available, or a null otherwise.
    * @see javax.transaction.TransactionManager
    */
   @Override
   public Object getTransactionIdentifier() {
      if (transactionManager == null) return null;

      Transaction transaction = null;
      try {
         transaction = transactionManager.getTransaction();
      }
      catch (SystemException e) {
         log.debug("Caught SystemException in TransactionalEventTransactionContext", e);
      }

      return transaction;
   }

   /**
    * Registers the synchronization passed in as a parameter with the ongoing transaction.
    * <p/>
    * If there is no ongoing transaction, then this method will do nothing and simply return.
    * <p/>
    *
    * @param synchronization synchronization to register.  Must not be null.
    * @throws NullPointerException if the synchronization is null.
    */
   @Override
   public void registerSynchronization(Synchronization synchronization) {
      if(transactionSynchronizationRegistry != null) {
         if (synchronization == null) throw new NullPointerException("Synchronization passed in is null!");
         transactionSynchronizationRegistry.registerInterposedSynchronization(synchronization);
      }
      else if (transactionManager != null) {
         if (synchronization == null) throw new NullPointerException("Synchronization passed in is null!");

         try {
            Transaction transaction = transactionManager.getTransaction();
            transaction.registerSynchronization(synchronization);
         }
         catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
   }
}
