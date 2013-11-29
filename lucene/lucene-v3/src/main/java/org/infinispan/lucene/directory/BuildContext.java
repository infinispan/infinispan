package org.infinispan.lucene.directory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.infinispan.lucene.readlocks.SegmentReadLocker;

/**
 * Building context to set construction parameters of Infinispan Directory instances
 *
 * @since 5.2
 * @author Sanne Grinovero
 */
public interface BuildContext {

   /**
    * Creates a Directory instance
    * @see org.apache.lucene.store.Directory
    * @return the new Directory
    */
   Directory create();

   /**
    * Sets the chunkSize option for the Directory being created.
    *
    * @param bytes segments are fragmented in chunkSize bytes; larger values are more efficient for searching but less for
    *        distribution and network replication
    * @return the same building context to eventually create the Directory instance
    */
   BuildContext chunkSize(int bytes);

   /**
    * Overrides the default SegmentReadLocker. In some cases you might be able to provide more efficient implementations than
    * the default one by controlling the IndexReader's lifecycle
    *
    * @see org.infinispan.lucene.readlocks
    * @param srl the new read locking strategy for fragmented segments
    * @return the same building context to eventually create the Directory instance
    */
   BuildContext overrideSegmentReadLocker(SegmentReadLocker srl);

   /**
    * Overrides the IndexWriter LockFactory
    *
    * @see org.infinispan.lucene.locking
    * @param lf the LockFactory to be used by IndexWriters.
    * @return the same building context to eventually create the Directory instance
    */
   BuildContext overrideWriteLocker(LockFactory lf);

}
