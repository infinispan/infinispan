package org.infinispan.client.hotrod.impl.transaction;

import javax.transaction.Transaction;

/**
 * A {@link Transaction} table that knows how to interact with the {@link Transaction} and how the {@link
 * TransactionalRemoteCacheImpl} is enlisted.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public interface TransactionTable {

   <K, V> TransactionContext<K, V> enlist(TransactionalRemoteCacheImpl<K, V> txRemoteCache, Transaction tx);

   /**
    * It initializes the {@link TransactionTable} with the {@link TransactionOperationFactory} to use.
    *
    * @param operationFactory The {@link TransactionOperationFactory} to use.
    */
   void start(TransactionOperationFactory operationFactory);
}
