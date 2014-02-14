package org.infinispan.lucene.readlocks;

/**
 * NoopSegmentReadLocker ignores requests to apply a readlock, but also ignores requests to delete files.
 * It might be a good choice for read-only indexes, or cases in which leaving unused segments in the index is
 * not considered a problem.
 *
 * @author Sanne Grinovero
 * @since 4.1
 */
public class NoopSegmentReadLocker implements SegmentReadLocker {

   /**
    * doesn't do anything and returns true
    */
   @Override
   public boolean acquireReadLock(String filename) {
      return true;
   }

   /**
    * doesn't do anything
    */
   @Override
   public void deleteOrReleaseReadLock(String filename) {
      return;
   }

}
