package org.infinispan.query.indexmanager;

/**
 * Interface to control the Lucene index's write lock.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
interface IndexLockController {

   /**
    * @return if the lock is available at the time of returning
    */
   boolean waitForAvailability();

   /**
    * Deletes the current index lock, even if anything is actively writing on the index:
    * this will not notify the writer in any way, so use with care.
    */
   void forceLockClear();

}
