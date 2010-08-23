package org.infinispan.util.concurrent.locks.containers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * A container for locks
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface LockContainer {
   /**
    * Tests if a give owner owns a lock on a specified object.
    *
    * @param key   object to check
    * @param owner owner to test
    * @return true if owner owns lock, false otherwise
    */
   boolean ownsLock(Object key, Object owner);

   /**
    * @param key object
    * @return true if an object is locked, false otherwise
    */
   boolean isLocked(Object key);

   /**
    * @param key object
    * @return the lock for a specific object
    */
   Lock getLock(Object key);

   /**
    * @return number of locks held
    */
   int getNumLocksHeld();

   /**
    * @return the size of the shared lock pool
    */
   int size();

   /**
    * Attempts to acquire a lock for the given object within certain time boundaries defined by the timeout and
    * time unit parameters.
    *
    * @param key Object to acquire lock on
    * @param timeout Time after which the lock acquisition will fail
    * @param unit Time unit of the given timeout
    * @return If lock was acquired it returns the corresponding Lock object. If lock was not acquired, it returns null
    * @throws InterruptedException If the lock acquisition was interrupted
    */
   Lock acquireLock(Object key, long timeout, TimeUnit unit) throws InterruptedException;

   /**
    * Release lock on the given key.
    *
    * @param key Object on which lock is to be removed  
    */
   void releaseLock(Object key);
}
