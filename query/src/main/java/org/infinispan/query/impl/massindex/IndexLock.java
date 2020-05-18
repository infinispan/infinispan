package org.infinispan.query.impl.massindex;

import java.util.concurrent.CompletionStage;

/**
 * The lock used to prevent multiple executions of the MassIndexer concurrently.
 *
 * @since 10.1
 */
interface IndexLock {

   /**
    * Tries to acquire a lock to execute the MassIndexer, without waiting.
    *
    * @return true if the lock was acquired, or false if it was already acquired.
    */
   CompletionStage<Boolean> lock();

   /**
    * Unlock the MassIndexer.
    */
   CompletionStage<Void> unlock();
}
