package org.infinispan.query.impl.massindex;

/**
 * The lock used to prevent multiple executions of the MassIndexer concurrently.
 *
 * @since 10.1
 */
interface MassIndexLock {

   /**
    * Tries to acquire a lock to execute the MassIndexer, without waiting.
    *
    * @return true if the lock was acquired, or false if it was already acquired.
    */
   boolean lock();

   /**
    * Unlock the MassIndexer.
    */
   void unlock();

   /**
    * @return true is the lock is acquired and thus the MassIndexer is already running.
    */
   boolean isAcquired();
}
