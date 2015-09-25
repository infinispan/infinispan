package org.infinispan.lucene.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Executor;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.readlocks.SegmentReadLocker;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Directory implementation for Apache Lucene.
 * Meant to be compatible with versions 5.0+
 *
 * @since 5.2
 * @author Sanne Grinovero
 * @see org.apache.lucene.store.Directory
 * @see org.apache.lucene.store.LockFactory
 */
class DirectoryLucene extends Directory implements DirectoryExtensions {

   private static final Log log = LogFactory.getLog(DirectoryLucene.class);
   private static final boolean trace = log.isTraceEnabled();

   private final DirectoryImplementor impl;

   // indexName is used to be able to store multiple named indexes in the same caches
   private final String indexName;
   private final Executor deleteExecutor;
   private final int affinitySegmentId;

   private volatile LockFactory lockFactory;

   /**
    * @param metadataCache the cache to be used for all smaller metadata: prefer replication over distribution, avoid eviction
    * @param chunksCache the cache to use for the space consuming segments: prefer distribution, enable eviction if needed
    * @param distLocksCache the cache to use for locks, to avoid more than one process to write to the index
    * @param indexName the unique index name, useful to store multiple indexes in the same caches
    * @param lf the LockFactory to be used by IndexWriters. @see org.infinispan.lucene.locking
    * @param chunkSize segments are fragmented in chunkSize bytes; larger values are more efficient for searching but less for distribution and network replication
    * @param readLocker @see org.infinispan.lucene.readlocks for some implementations; you might be able to provide more efficient implementations by controlling the IndexReader's lifecycle.
    * @param fileListUpdatedAsync When true, the writes to the list of currently existing files in the Directory will use the putAsync method rather than put.
    * @param deleteExecutor The Executor to run file deletes in the background
    * @param affinitySegmentId A hint interpreted by the consistent hashing function to force locality with a specific segment identifier
    */
   public DirectoryLucene(Cache<?, ?> metadataCache, Cache<?, ?> chunksCache, Cache<?, ?> distLocksCache, String indexName, LockFactory lf, int chunkSize, SegmentReadLocker readLocker, boolean fileListUpdatedAsync, Executor deleteExecutor, int affinitySegmentId) {
      this.deleteExecutor = deleteExecutor;
      this.affinitySegmentId = affinitySegmentId;
      this.impl = new DirectoryImplementor(metadataCache, chunksCache, distLocksCache, indexName, chunkSize, readLocker, fileListUpdatedAsync, affinitySegmentId);
      this.indexName = indexName;
      this.lockFactory = lf;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void deleteFile(final String name) {
      ensureOpen();
      deleteExecutor.execute(new DeleteTask(name));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void renameFile(final String from, final String to) {
      impl.renameFile(from, to);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public long fileLength(final String name) {
      ensureOpen();
      return impl.fileLength(name);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
      return impl.createOutput(name);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public IndexInput openInput(final String name, final IOContext context) throws IOException {
      final IndexInputContext indexInputContext = impl.openInput(name);
      if (indexInputContext.readLocks == null) {
         return new SingleChunkIndexInput(indexInputContext);
      } else {
         return new InfinispanIndexInput(indexInputContext);
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void close() {
      // Note the we don't really keep track of this anymore
   }

   @Override
   public String toString() {
      return "InfinispanDirectory{indexName=\'" + indexName + "\'}";
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String[] listAll() {
      return impl.list();
   }

   /**
    * @return The value of indexName, same constant as provided to the constructor.
    */
   @Override
   public String getIndexName() {
      return indexName;
   }

   public int getAffinitySegmentId() {
      return affinitySegmentId;
   }
   /**
    * {@inheritDoc}
    */
   @Override
   public void sync(Collection<String> names) throws IOException {
      //This implementation is always in sync with the storage, so NOOP is fine
   }

   @Override
   public Lock obtainLock(String lockName) throws IOException {
      return lockFactory.obtainLock(this, lockName);
   }

   @Override
   public int getChunkSize() {
      return impl.getChunkSize();
   }

   @Override
   public Cache getMetadataCache() {
      return impl.getMetadataCache();
   }

   @Override
   public Cache getDataCache() {
      return impl.getDataCache();
   }

   /**
    * Force release of the lock in this directory. Make sure to understand the
    * consequences
    */
   @Override
   public void forceUnlock(String lockName) {
      Cache<Object, Integer> lockCache = getDistLockCache().getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE, Flag.SKIP_CACHE_LOAD);
      FileCacheKey fileCacheKey = new FileCacheKey(indexName, lockName, affinitySegmentId);
      Object previousValue = lockCache.remove(fileCacheKey);
      if (previousValue!=null && trace) {
         log.tracef("Lock forcibly removed for index: %s", indexName);
      }
   }

   public Cache<Object, Integer> getDistLockCache() {
      return impl.getDistLocksCache();
   }

   final class DeleteTask implements Runnable {

      private final String fileName;

      private DeleteTask(String fileName) {
         this.fileName = fileName;
      }

      public String getFileName() {
         return fileName;
      }

      @Override
      public void run() {
         impl.deleteFile(fileName);
      }
   }

}
