/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.lucene;

import java.io.IOException;
import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Factory for locks obtained in <code>InfinispanDirectory</code>
 * 
 * @author Lukasz Moren
 * @see org.hibernate.search.store.infinispan.InfinispanDirectory
 * @see org.hibernate.search.store.infinispan.InfinispanLockFactory.InfinispanLock
 */
public class InfinispanLockFactory extends LockFactory {

   private static final Log log = LogFactory.getLog(InfinispanLockFactory.class);

   private Cache<CacheKey, Object> cache;
   private String indexName;

   public InfinispanLockFactory(Cache<CacheKey, Object> cache, String indexName) {
      this.cache = cache;
      this.indexName = indexName;
   }

   /**
    * {@inheritDoc}
    */
   public Lock makeLock(String lockName) {
      try {
         return new InfinispanLock(cache, indexName, lockName);
      } finally {
         if (log.isTraceEnabled()) {
            log.trace("Created new lock: {} for index {}", lockName, indexName);
         }
      }
   }

   /**
    * {@inheritDoc}
    */
   public void clearLock(String lockName) throws IOException {
      try {
         cache.remove(new FileCacheKey(indexName, lockName, true));
      } finally {
         if (log.isTraceEnabled()) {
            log.trace("Removed lock: {} for index {}", lockName, indexName);
         }
      }
   }

   /**
    * Interprocess Lucene index lock
    * 
    * @see org.apache.lucene.store.Directory#makeLock(String)
    */
   public static class InfinispanLock extends Lock {

      private static final Log log = LogFactory.getLog(InfinispanLock.class);

      private final Cache<CacheKey, Object> cache;
      private String lockName;
      private String indexName;

      final TransactionManager tm;

      InfinispanLock(Cache<CacheKey, Object> cache, String indexName, String lockName) {
         this.cache = cache;
         this.lockName = lockName;
         this.indexName = indexName;

         tm = cache.getAdvancedCache().getComponentRegistry().getComponent(TransactionManager.class);
         if (tm == null) {
            throw new CacheException(
                     "Failed looking up TransactionManager, check if any transaction manager is associated with Infinispan cache: "
                              + cache.getName());
         }
      }

      /**
       * {@inheritDoc}
       */
      public boolean obtain() throws IOException {
         boolean acquired = false;

         synchronized (cache) {
            try {
               // begin transaction for lock obtaining
               tm.begin();
               CacheKey lock = new FileCacheKey(indexName, lockName, true);
               if (!cache.containsKey(lock)) {
                  cache.put(lock, lock);
                  acquired = true;
               }
            } catch (Exception e) {
               log.error("Cannot obtain lock for: " + indexName, e);
            } finally {
               try {
                  if (tm.getTransaction() != null) {
                     if (acquired) {
                        tm.commit();
                        if (log.isTraceEnabled()) {
                           log.trace("Lock: {} acquired for index: {} ", new Object[] { lockName, indexName });
                        }
                     } else {
                        tm.rollback();
                     }
                  }
               } catch (RollbackException e) {
                  log.error("Cannot obtain lock for: " + indexName, e);
                  acquired = false;
               } catch (Exception e) {
                  throw new CacheException(e);
               }
            }

            if (acquired) {
               try {
                  // begin new transaction to batch all changes, tx commited when lock is released.
                  tm.begin();
                  if (log.isTraceEnabled()) {
                     log.trace("Batch transaction started for index: {}", indexName);
                  }
               } catch (Exception e) {
                  log.error("Unable to start transaction", e);
               }
            }
         }

         return acquired;
      }

      /**
       * {@inheritDoc}
       */
      public void release() throws IOException {
         boolean removed = false;
         synchronized (cache) {
            try {
               // commit changes in batch, transaction was started when lock was acquired
               tm.commit();
               if (log.isTraceEnabled()) {
                  log.trace("Batch transaction commited for index: {}", indexName);
               }

               tm.begin();
               removed = cache.remove(new FileCacheKey(indexName, lockName, true)) != null;
            } catch (Exception e) {
               throw new CacheException("Unable to commit work done or release lock!", e);
            } finally {
               try {
                  if (removed) {
                     tm.commit();
                     if (log.isTraceEnabled()) {
                        log.trace("Lock: {} removed for index: {} ", new Object[] { lockName, indexName });
                     }
                  } else {
                     tm.rollback();
                  }
               } catch (Exception e) {
                  throw new CacheException("Unable to release lock!", e);
               }
            }
         }
      }

      /**
       * {@inheritDoc}
       */
      public boolean isLocked() {
         boolean locked = false;
         synchronized (cache) {
            Transaction tx = null;
            try {
               // if there is an ongoing transaction we need to suspend it
               if ((tx = tm.getTransaction()) != null) {
                  tm.suspend();
               }
               locked = cache.containsKey(new FileCacheKey(indexName, lockName, true));
            } catch (Exception e) {
               log.error("Error in suspending transaction", e);
            } finally {
               if (tx != null) {
                  try {
                     tm.resume(tx);
                  } catch (Exception e) {
                     throw new CacheException("Unable to resume suspended transaction " + tx, e);
                  }
               }
            }
         }
         return locked;
      }
   }

}
