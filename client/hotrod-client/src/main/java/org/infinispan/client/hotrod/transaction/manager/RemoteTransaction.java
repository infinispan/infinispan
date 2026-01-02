package org.infinispan.client.hotrod.transaction.manager;

import org.infinispan.commons.tx.TransactionImpl;

import jakarta.transaction.Transaction;

/**
 * A {@link Transaction} implementation used by {@link RemoteTransactionManager}.
 *
 * @author Pedro Ruivo
 * @since 9.3
 * @see RemoteTransactionManager
 */
final class RemoteTransaction extends TransactionImpl {

   RemoteTransaction(RemoteTransactionManager transactionManager) {
      super();
      setXid(RemoteXid.create(transactionManager.getTransactionManagerId()));
   }

}
