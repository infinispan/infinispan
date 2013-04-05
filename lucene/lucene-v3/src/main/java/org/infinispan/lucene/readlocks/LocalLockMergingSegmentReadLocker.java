/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.lucene.readlocks;

import org.infinispan.Cache;
import org.infinispan.util.CollectionFactory;

import java.util.concurrent.ConcurrentMap;

/**
 * LocalLockMergingSegmentReadLocker decorates the {@link DistributedSegmentReadLocker} to minimize
 * remote operations in case several IndexReaders are opened on the same Infinispan based {@link Directory}.
 * It keeps track of locks which where already acquired for a specific filename from another request on
 * the same node and merges the request so that the different clients share the same remote lock.
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
@SuppressWarnings("unchecked")
public class LocalLockMergingSegmentReadLocker implements SegmentReadLocker {

   private final ConcurrentMap<String, LocalReadLock> localLocks = CollectionFactory.makeConcurrentMap();
   private final DistributedSegmentReadLocker delegate;

   /**
    * Create a new LocalLockMergingSegmentReadLocker for specified cache and index name.
    * 
    * @param cache
    * @param indexName
    */
   public LocalLockMergingSegmentReadLocker(Cache<?, ?> cache, String indexName) {
      this.delegate = new DistributedSegmentReadLocker((Cache<Object, Integer>) cache, cache, cache, indexName);
   }
   
   /**
    * Create a new LocalLockMergingSegmentReadLocker with special purpose caches
    * @param locksCache the cache to be used to store distributed locks
    * @param chunksCache the cache containing the chunks, this is where the bulk of data is stored
    * @param metadataCache smaller cache for the metadata of stored elements
    * @param indexName
    */
   public LocalLockMergingSegmentReadLocker(Cache<?, ?> locksCache, Cache<?, ?> chunksCache, Cache<?, ?> metadataCache, String indexName) {
      this.delegate = new DistributedSegmentReadLocker((Cache<Object, Integer>) locksCache, chunksCache, metadataCache, indexName);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean acquireReadLock(String name) {
      LocalReadLock localReadLock = getLocalLockByName(name);
      boolean acquired = localReadLock.acquire();
      if (acquired) {
         return true;
      }
      else {
         // cleanup
         localLocks.remove(name);
         return false;
      }
   }
   
   private LocalReadLock getLocalLockByName(String name) {
      LocalReadLock localReadLock = localLocks.get(name);
      if (localReadLock == null) {
         LocalReadLock newReadLock = new LocalReadLock(name);
         LocalReadLock prevReadLock = localLocks.putIfAbsent(name, newReadLock);
         localReadLock = prevReadLock == null ? newReadLock : prevReadLock;
      }
      return localReadLock;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void deleteOrReleaseReadLock(String name) {
      getLocalLockByName(name).release();
   }
   
   private class LocalReadLock {
      private final String name;
      private int value = 0;

      LocalReadLock(String name) {
         this.name = name;
      }

      /**
       * @return true if the lock was acquired, false if it's too late: the file
       * was deleted and this LocalReadLock should be removed too.
       */
      synchronized boolean acquire() {
         if (value == 0) {
            boolean haveIt = delegate.acquireReadLock(name);
            if (haveIt) {
               value = 1;
               return true;
            } else {
               value = -1;
               return false;
            }
         } else if (value == -1) {
            // it was deleted just a two lines ago
            return false;
         } else {
            value++;
            return true;
         }
      }
      
      synchronized void release() {
         value--;
         if (value <= 0) {
            localLocks.remove(name);
            delegate.deleteOrReleaseReadLock(name);
         }
      }
   }

}
