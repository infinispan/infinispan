package org.infinispan.persistence.spi;

import org.infinispan.persistence.support.BatchModification;

import javax.transaction.Transaction;

/**
 * Defines the functionality of a transactional store.  This interface allows the implementing store to participate in the
 * 2PC protocol of a cache's transaction. This enables the cache transaction to be rolledback if an exception occurs whilst
 * writing key changes to the underlying store, or for the writes to the underlying store to be rolledback if the exception
 * occurs in-memory.
 *
 * As this writer is part of the 2PC, all writes to the underlying store should only be executed by the originator of a
 * transaction in normal operation.  In the event that the originator crashes between the prepare and commit/rollback phase
 * it is expected that the underlying store's transaction will eventually timeout and rollback. In the event that the originator
 * crashes and transaction recovery is enabled, then forcing commit will result in the replaying of said Tx's (prepare/commit) to
 * the underlying store.
 *
 * @author Ryan Emerson
 * @since 9.0
 */
public interface TransactionalCacheWriter<K, V> extends AdvancedCacheWriter<K, V> {

   /**
    * Write modifications to the store in the prepare phase, as this is the only way we know the FINAL values of the entries.
    * This is required to handle scenarios where an objects value is changed after the put command has been executed, but
    * before the commit is called on the Tx.
    *
    * @param transaction the current transactional context.
    * @param batchModification an object containing the write/remove operations required for this transaction.
    * @throws PersistenceException if an error occurs when communicating/performing writes on the underlying store.
    */
   void prepareWithModifications(Transaction transaction, BatchModification batchModification) throws PersistenceException;

   /**
    * Commit the provided transaction's changes to the underlying store.
    *
    * @param transaction the current transactional context.
    */
   void commit(Transaction transaction);

   /**
    * Rollback the provided transaction's changes to the underlying store.
    *
    * @param transaction the current transactional context.
    */
   void rollback(Transaction transaction);
}
