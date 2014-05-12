package org.infinispan.lucene.impl;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.lucene.readlocks.SegmentReadLocker;

/**
 * Directory implementation for Apache Lucene.
 * Meant to be compatible with the versions from 4.0 to 4.6.
 *
 * @since 5.2
 * @author Sanne Grinovero
 * @see org.apache.lucene.store.Directory
 * @see org.apache.lucene.store.LockFactory
 */
class DirectoryLuceneV4 extends Directory implements DirectoryExtensions {

   private final DirectoryImplementor impl;

   // indexName is used to be able to store multiple named indexes in the same caches
   private final String indexName;

   private volatile LockFactory lockFactory;

   /**
    * @param metadataCache the cache to be used for all smaller metadata: prefer replication over distribution, avoid eviction
    * @param chunksCache the cache to use for the space consuming segments: prefer distribution, enable eviction if needed
    * @param indexName the unique index name, useful to store multiple indexes in the same caches
    * @param lf the LockFactory to be used by IndexWriters. @see org.infinispan.lucene.locking
    * @param chunkSize segments are fragmented in chunkSize bytes; larger values are more efficient for searching but less for distribution and network replication
    * @param readLocker @see org.infinispan.lucene.readlocks for some implementations; you might be able to provide more efficient implementations by controlling the IndexReader's lifecycle.
    */
   public DirectoryLuceneV4(Cache<?, ?> metadataCache, Cache<?, ?> chunksCache, String indexName, LockFactory lf, int chunkSize, SegmentReadLocker readLocker) {
      this.impl = new DirectoryImplementorV4(metadataCache, chunksCache, indexName, chunkSize, readLocker);
      this.indexName = indexName;
      this.lockFactory = lf;
      this.lockFactory.setLockPrefix(this.getLockID());
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean fileExists(final String name) {
      ensureOpen();
      return impl.fileExists(name);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void deleteFile(final String name) {
      ensureOpen();
      impl.deleteFile(name);
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
      if ( indexInputContext.readLocks == null ) {
         return new SingleChunkIndexInput(indexInputContext);
      }
      else {
         return new InfinispanIndexInputV4(indexInputContext);
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

   /**
    * {@inheritDoc}
    */
   @Override
   public void sync(Collection<String> names) throws IOException {
      //This implementation is always in sync with the storage, so NOOP is fine
   }

   //@Override New method since Lucene 4.6
   public void clearLock(String lockName) throws IOException {
      lockFactory.clearLock(lockName);
   }

   //@Override New method since Lucene 4.6
   public LockFactory getLockFactory() {
      return lockFactory;
   }

   //@Override New method since Lucene 4.6
   public Lock makeLock(String lockName) {
      return lockFactory.makeLock(lockName);
   }

   //@Override New method since Lucene 4.6
   public void setLockFactory(LockFactory lockFactory) throws IOException {
      this.lockFactory = lockFactory;
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

}
