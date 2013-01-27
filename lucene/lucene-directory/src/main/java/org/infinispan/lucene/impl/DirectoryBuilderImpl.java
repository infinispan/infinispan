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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.lucene.directory.BuildContext;
import org.infinispan.lucene.locking.BaseLockFactory;
import org.infinispan.lucene.readlocks.DistributedSegmentReadLocker;
import org.infinispan.lucene.readlocks.SegmentReadLocker;

public class DirectoryBuilderImpl implements BuildContext {

   /**
    * Used as default chunk size: each Lucene index segment is splitted into smaller parts having a default size in bytes as
    * defined here
    */
   public final static int DEFAULT_BUFFER_SIZE = 16 * 1024;

   /**
    * Mandatory parameters:
    */

   private final Cache<?, ?> metadataCache;
   private final Cache<?, ?> chunksCache;
   private final Cache<?, ?> distLocksCache;
   private final String indexName;

   /**
    * Optional parameters:
    */

   private int chunkSize = DEFAULT_BUFFER_SIZE;
   private SegmentReadLocker srl = null;
   private LockFactory lockFactory = null;

   public DirectoryBuilderImpl(Cache<?, ?> metadataCache, Cache<?, ?> chunksCache, Cache<?, ?> distLocksCache, String indexName) {
      checkNotNull(metadataCache, "metadataCache");
      checkNotNull(chunksCache, "chunksCache");
      checkNotNull(distLocksCache, "distLocksCache");
      checkNotNull(indexName, "indexName");
      this.metadataCache = metadataCache;
      this.chunksCache = chunksCache;
      this.distLocksCache = distLocksCache;
      this.indexName = indexName;
   }

   @Override
   public Directory create() {
      if (lockFactory == null) {
         lockFactory = makeDefaultLockFactory(distLocksCache, indexName);
      }
      if (srl == null) {
         srl = makeDefaultSegmentReadLocker(metadataCache, chunksCache, distLocksCache, indexName);
      }
      return new InfinispanDirectory(metadataCache, chunksCache, indexName, lockFactory, chunkSize, srl);
   }

   @Override
   public BuildContext chunkSize(int bytes) {
      if (bytes <= 0)
         throw new IllegalArgumentException("chunkSize must be a positive integer");
      this.chunkSize = bytes;
      return this;
   }

   @Override
   public BuildContext overrideSegmentReadLocker(SegmentReadLocker srl) {
      checkNotNull(srl, "srl");
      this.srl = srl;
      return this;
   }

   @Override
   public BuildContext overrideWriteLocker(LockFactory lockFactory) {
      checkNotNull(lockFactory, "lockFactory");
      this.lockFactory = lockFactory;
      return this;
   }

   static SegmentReadLocker makeDefaultSegmentReadLocker(Cache<?, ?> metadataCache, Cache<?, ?> chunksCache, Cache<?, ?> distLocksCache, String indexName) {
      checkNotNull(distLocksCache, "distLocksCache");
      checkNotNull(indexName, "indexName");
      return new DistributedSegmentReadLocker((Cache<Object, Integer>) distLocksCache, chunksCache, metadataCache, indexName);
   }

   static void checkNotNull(Object v, String objectname) {
      if (v == null)
         throw new IllegalArgumentException(objectname + " must not be null");
   }

   static LockFactory makeDefaultLockFactory(Cache<?, ?> cache, String indexName) {
      checkNotNull(cache, "cache");
      checkNotNull(indexName, "indexName");
      return new BaseLockFactory(cache, indexName);
   }

}
