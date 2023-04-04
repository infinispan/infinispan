package org.infinispan.hotrod.transaction.manager;

import jakarta.transaction.Transaction;

import org.infinispan.commons.tx.TransactionImpl;

/**
 * A {@link Transaction} implementation used by {@link RemoteTransactionManager}.
 *
 * @since 14.0
 * @see RemoteTransactionManager
 */
final class RemoteTransaction extends TransactionImpl {

   RemoteTransaction(RemoteTransactionManager transactionManager) {
      super();
      setXid(RemoteXid.create(transactionManager.getTransactionManagerId()));
   }

}
