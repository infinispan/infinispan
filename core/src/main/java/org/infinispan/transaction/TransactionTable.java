package org.infinispan.transaction;

import java.util.Collection;

import org.infinispan.transaction.xa.GlobalTransaction;

import jakarta.transaction.Transaction;

/**
 * Interface that allows to fetch the {@link org.infinispan.transaction.xa.GlobalTransaction} associated to local or
 * remote transactions.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public interface TransactionTable {

   /**
    * @param transaction the local transaction. Must be non-null.
    * @return the {@link org.infinispan.transaction.xa.GlobalTransaction} associated with the transaction or {@code
    * null} if it doesn't exist.
    */
   GlobalTransaction getGlobalTransaction(Transaction transaction);

   /**
    * @return an unmodified collection of {@link org.infinispan.transaction.xa.GlobalTransaction} associated with local
    * running transactions.
    */
   Collection<GlobalTransaction> getLocalGlobalTransaction();

   /**
    * @return an unmodified collection of {@link org.infinispan.transaction.xa.GlobalTransaction} associated with remote
    * transactions.
    */
   Collection<GlobalTransaction> getRemoteGlobalTransaction();

}
