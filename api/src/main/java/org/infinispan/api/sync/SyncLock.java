package org.infinispan.api.sync;

import java.util.concurrent.TimeUnit;

/**
 * @since 14.0
 **/
public interface SyncLock {

   String name();

   /**
    * Return the container of this lock
    * @return
    */
   SyncContainer container();

   void lock();

   boolean tryLock();

   boolean tryLock(long time, TimeUnit unit);

   void unlock();

   boolean isLocked();

   boolean isLockedByMe();
}
