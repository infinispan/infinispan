package org.infinispan.lucene.directory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.infinispan.lucene.readlocks.SegmentReadLocker;

import java.util.concurrent.Executor;

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

   /**
    * When set to true, the list of files of the Directory is propagated to other nodes
    * asynchronously.
    * This implies that a committed change to the index will not immediately be accessible
    * by searching threads on other nodes, but the gap in time is not longer than the
    * time of a single RPC so this gap should not be measurable unless there is some
    * form of congestion.
    * Currently defaults to false as it's safer.
    *
    * @param writeFileListAsync
    * @return the same building context to eventually create the Directory instance
    * @experimental
    */
   BuildContext writeFileListAsynchronously(boolean writeFileListAsync);

   /**
    * Provides an Executor to handle delete operations in a background thread
    *
    * @param executor
    * @return
    */
   BuildContext deleteOperationsExecutor(Executor executor);

}
