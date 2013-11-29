package org.infinispan.lucene.readlocks;

/**
 * <p>SegmentReadLocker implementations have to make sure that segments are not deleted while they are
 * being used by an IndexReader.</p>
 * <p>When an {@link org.infinispan.lucene.impl.InfinispanIndexInput} is opened on a file which is split in smaller chunks,
 * {@link #acquireReadLock(String)} is invoked; then the {@link #deleteOrReleaseReadLock(String)} is
 * invoked when the stream is closed.</p>
 * <p>The same {@link #deleteOrReleaseReadLock(String)} is invoked when a file is deleted, so if this invocation is not balancing
 * a lock acquire this implementation must delete all segment chunks and the associated metadata.</p>
 * <p>Note that if you can use and tune the {@link org.apache.lucene.index.LogByteSizeMergePolicy} you could avoid the need
 * for readlocks by setting a maximum segment size to equal the chunk size used by the InfinispanDirectory; readlocks
 * will be skipped automatically when not needed, so it's advisable to still configure an appropriate SegmentReadLocker
 * for the cases you might want to tune the chunk size.</p>
 *
 * @author Sanne Grinovero
 * @since 4.1
 */
public interface SegmentReadLocker {

   /**
    * It will release a previously acquired readLock, or
    * if no readLock was acquired it will mark the file to be deleted as soon
    * as all pending locks are releases.
    * If it's invoked on a file without pending locks the file is deleted.
    *
    * @param fileName of the file to release or delete
    * @see Directory#deleteFile(String)
    */
   void deleteOrReleaseReadLock(String fileName);

   /**
    * Acquires a readlock, in order to prevent other invocations to {@link #deleteOrReleaseReadLock(String)}
    * from deleting the file.
    *
    * @param filename
    * @return true if the lock was acquired, false if the implementation
    * detects the file does not exist, or that it's being deleted by some other thread.
    * @see Directory#openInput(String)
    */
   boolean acquireReadLock(String filename);

}
