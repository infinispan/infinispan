package org.infinispan.lucene.impl;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.lucene.readlocks.SegmentReadLocker;

/**
 * An implementation of Lucene's {@link org.apache.lucene.store.Directory} which uses Infinispan to store Lucene indexes.
 * As the RAMDirectory the data is stored in memory, but provides some additional flexibility:
 * <p><b>Passivation, LRU or LIRS</b> Bigger indexes can be configured to passivate cleverly selected chunks of data to a cache store.
 * This can be a local filesystem, a network filesystem, a database or custom cloud stores like S3. See Infinispan's core documentation for a full list of available implementations, or {@link org.infinispan.persistence.spi.CacheWriter} to implement more.</p>
 * <p><b>Non-volatile memory</b> The contents of the index can be stored in it's entirety in such a store, so that on shutdown or crash of the system data is not lost.
 * A copy of the index will be copied to the store in sync or async depending on configuration; In case you enable
 * Infinispan's clustering even in case of async the segments are always duplicated synchronously to other nodes, so you can
 * benefit from good reliability even while choosing the asynchronous mode to write the index to the slowest store implementations.</p>
 * <p><b>Real-time change propagation</b> All changes done on a node are propagated at low latency to other nodes of the cluster; this was designed especially for
 * interactive usage of Lucene, so that after an IndexWriter commits on one node new IndexReaders opened on any node of the cluster
 * will be able to deliver updated search results.</p>
 * <p><b>Distributed heap</b> Infinispan acts as a shared heap for the purpose of total memory consumption, so you can avoid hitting the slower disks even
 * if the total size of the index can't fit in the memory of a single node: network is faster than disks, especially if the index
 * is bigger than the memory available to cache it.</p>
 * <p><b>Distributed locking</b>
 * As default Lucene Directory implementations a global lock needs to protect the index from having more than an IndexWriter open; in case of a
 * replicated or distributed index you need to enable a cluster-wide {@link org.apache.lucene.store.LockFactory}.
 * This implementation uses by default {@link org.infinispan.lucene.locking.BaseLockFactory}; in case you want to apply changes during a JTA transaction
 * see also {@link org.infinispan.lucene.locking.TransactionalLockFactory}.
 * </p>
 * <p><b>Combined store patterns</b> It's possible to combine different stores and passivation policies, so that each nodes shares the index changes
 * quickly to other nodes, offloads less frequently used data to a per-node local filesystem, and the cluster also coordinates to keeps a safe copy on a shared store.</p>
 *
 * @since 4.0
 * @author Sanne Grinovero
 * @author Lukasz Moren
 * @see org.apache.lucene.store.Directory
 * @see org.apache.lucene.store.LockFactory
 * @see org.infinispan.lucene.locking.BaseLockFactory
 * @see org.infinispan.lucene.locking.TransactionalLockFactory
 */
class DirectoryLuceneV3 extends Directory implements DirectoryExtensions {

   private final DirectoryImplementor impl;

   // indexName is used to be able to store multiple named indexes in the same caches
   private final String indexName;

   DirectoryLuceneV3(Cache<?, ?> metadataCache, Cache<?, ?> chunksCache, String indexName, LockFactory lf, int chunkSize, SegmentReadLocker readLocker) {
      this.impl = new DirectoryImplementor(metadataCache, chunksCache, indexName, chunkSize, readLocker);
      this.indexName = indexName;
      this.lockFactory = lf;
      this.lockFactory.setLockPrefix(this.getLockID());
   }

   //Overrides in some versions of Lucene
   public String[] list() {
      ensureOpen();
      return impl.list();
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
   public long fileModified(final String name) {
      ensureOpen();
      return impl.fileModified(name);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void touchFile(final String fileName) {
      ensureOpen();
      impl.touchFile(fileName);
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
   public IndexOutput createOutput(final String name) {
      return impl.createOutput(name);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public IndexInput openInput(final String name) throws IOException {
      final IndexInputContext indexInputContext = impl.openInput(name);
      if ( indexInputContext.readLocks == null ) {
         return new SingleChunkIndexInput(indexInputContext);
      }
      else {
         return new InfinispanIndexInputV3(indexInputContext);
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void close() {
      isOpen = false;
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
      return list();
   }

   /**
    * @return The value of indexName, same constant as provided to the constructor.
    */
   @Override
   public String getIndexName() {
       return indexName;
   }

}
