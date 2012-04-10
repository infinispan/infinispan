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
package org.infinispan.lucene.locking;

import org.apache.lucene.store.Lock;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Inter-IndexWriter Lucene index lock based on Infinispan.
 * This implementation is not bound to and does not need a TransactionManager,
 * is more suited for large batch work and index optimization.
 * 
 * @since 4.0
 * @author Sanne Grinovero
 * @see org.apache.lucene.store.Lock
 */
@SuppressWarnings("unchecked")
class BaseLuceneLock extends Lock {

   private static final Log log = LogFactory.getLog(BaseLuceneLock.class);

   private final Cache<Object, Object> noCacheStoreCache;
   private final String lockName;
   private final String indexName;
   private final FileCacheKey keyOfLock;

   BaseLuceneLock(Cache<?, ?> cache, String indexName, String lockName) {
      this.noCacheStoreCache = (Cache<Object, Object>) cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE, Flag.SKIP_CACHE_LOAD);
      this.lockName = lockName;
      this.indexName = indexName;
      this.keyOfLock = new FileCacheKey(indexName, lockName);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean obtain() {
      Object previousValue = noCacheStoreCache.putIfAbsent(keyOfLock, keyOfLock);
      if (previousValue == null) {
         if (log.isTraceEnabled()) {
            log.tracef("Lock: %s acquired for index: %s", lockName, indexName);
         }
         // we own the lock:
         return true;
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("Lock: %s not aquired for index: %s, was taken already.", lockName, indexName);
         }
         return false;
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void release() {
      clearLock();
   }

   /**
    * Used by Lucene at Directory creation: we expect the lock to not exist in this case.
    */
   public void clearLock() {
      Object previousValue = noCacheStoreCache.remove(keyOfLock);
      if (previousValue!=null && log.isTraceEnabled()) {
         log.tracef("Lock removed for index: %s", indexName);
      }
   }
   
   @Override
   public boolean isLocked() {
      boolean locked = noCacheStoreCache.containsKey(keyOfLock);
      return locked;
   }
   
}