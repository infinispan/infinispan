/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.lucene.impl;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.lucene.readlocks.SegmentReadLocker;

/**
 * Directory implementation for Apache Lucene 4.0 and 4.1
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

   /**
    * @param metadataCache the cache to be used for all smaller metadata: prefer replication over distribution, avoid eviction
    * @param chunksCache the cache to use for the space consuming segments: prefer distribution, enable eviction if needed
    * @param indexName the unique index name, useful to store multiple indexes in the same caches
    * @param lf the LockFactory to be used by IndexWriters. @see org.infinispan.lucene.locking
    * @param chunkSize segments are fragmented in chunkSize bytes; larger values are more efficient for searching but less for distribution and network replication
    * @param readLocker @see org.infinispan.lucene.readlocks for some implementations; you might be able to provide more efficient implementations by controlling the IndexReader's lifecycle.
    */
   public DirectoryLuceneV4(Cache<?, ?> metadataCache, Cache<?, ?> chunksCache, String indexName, LockFactory lf, int chunkSize, SegmentReadLocker readLocker) {
      this.impl = new DirectoryImplementor(metadataCache, chunksCache, indexName, chunkSize, readLocker);
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
      return impl.openInput(name);
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

}
