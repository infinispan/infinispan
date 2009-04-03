package org.horizon.util.concurrent.locks.containers;

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

   boolean acquireLock(Object key, long timeout, TimeUnit unit) throws InterruptedException;

   void releaseLock(Object key);
}
