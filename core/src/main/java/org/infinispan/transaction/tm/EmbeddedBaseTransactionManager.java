package org.infinispan.transaction.tm;


import java.util.UUID;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.commons.tx.TransactionManagerImpl;

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
 * @since 9.0
 */
public class EmbeddedBaseTransactionManager extends TransactionManagerImpl {

   UUID getTransactionManagerId() {
      return transactionManagerId;
   }

   @Override
   protected EmbeddedTransaction createTransaction() {
      return new EmbeddedTransaction(this);
   }

   @Override
   public EmbeddedTransaction getTransaction() {
      return (EmbeddedTransaction) super.getTransaction();
   }
}
