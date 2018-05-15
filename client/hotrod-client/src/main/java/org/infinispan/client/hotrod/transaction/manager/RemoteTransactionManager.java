package org.infinispan.client.hotrod.transaction.manager;

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
 * @author Pedro Ruivo
 * @since 9.3
 */
public final class RemoteTransactionManager extends TransactionManagerImpl {

   private RemoteTransactionManager() {
      super();
   }

   public static RemoteTransactionManager getInstance() {
      return LazyInitializeHolder.INSTANCE;
   }

   @Override
   protected Transaction createTransaction() {
      return new RemoteTransaction(this);
   }

   UUID getTransactionManagerId() {
      return transactionManagerId;
   }

   private static class LazyInitializeHolder {
      static final RemoteTransactionManager INSTANCE = new RemoteTransactionManager();
   }
}
