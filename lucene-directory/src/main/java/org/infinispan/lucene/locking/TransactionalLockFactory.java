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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.TransactionManager;

/**
 * <p>Factory for locks obtained in <code>InfinispanDirectory</code>,
 * this factory produces instances of <code>TransactionalSharedLuceneLock</code>.</p>
 * <p>Usually Lucene acquires the lock when creating an IndexWriter and releases it
 * when closing it; these open-close operations are mapped to transactions as begin-commit,
 * so all changes are going to be effective at IndexWriter close.
 * The advantage is that a transaction rollback will be able to undo all changes
 * applied to the index, but this requires enough memory to hold all the changes until
 * the commit.</p>
 * <p>Using a TransactionalSharedLuceneLock is not compatible with Lucene's
 * default MergeScheduler: use an in-thread implementation like SerialMergeScheduler
 * <code>indexWriter.setMergeScheduler( new SerialMergeScheduler() );</code></p>
 * 
 * @since 4.0
 * @author Sanne Grinovero
 * @author Lukasz Moren
 * @see org.infinispan.lucene.InfinispanDirectory
 * @see org.infinispan.lucene.locking.TransactionalSharedLuceneLock
 * @see org.apache.lucene.index.SerialMergeScheduler
 */
@SuppressWarnings("unchecked")
public class TransactionalLockFactory extends LockFactory {

   private static final Log log = LogFactory.getLog(TransactionalLockFactory.class);
   private static final String DEF_LOCK_NAME = IndexWriter.WRITE_LOCK_NAME;

   private final Cache<?, ?> cache;
   private final String indexName;
   private final TransactionManager tm;
   private final TransactionalSharedLuceneLock defLock;

   public TransactionalLockFactory(Cache<?, ?> cache, String indexName) {
      this.cache = cache;
      this.indexName = indexName;
      tm = cache.getAdvancedCache().getTransactionManager();
      if (tm == null) {
         ComponentStatus status = cache.getAdvancedCache().getComponentRegistry().getStatus();
         if (status.equals(ComponentStatus.RUNNING)) {
            throw new CacheException(
                     "Failed looking up TransactionManager. Check if any transaction manager is associated with Infinispan cache: \'"
                              + cache.getName() + "\'");
         }
         else {
            throw new CacheException("Failed looking up TransactionManager: the cache is not running");
         }
      }
      defLock = new TransactionalSharedLuceneLock(cache, indexName, DEF_LOCK_NAME, tm);
   }

   /**
    * {@inheritDoc}
    */
   public TransactionalSharedLuceneLock makeLock(String lockName) {
      TransactionalSharedLuceneLock lock;
      //It appears Lucene always uses the same name so we give locks
      //having this name a special treatment:
      if (DEF_LOCK_NAME.equals(lockName)) {
         lock = defLock;
      }
      else {
         // this branch is never taken with current Lucene version.
         lock = new TransactionalSharedLuceneLock(cache, indexName, lockName, tm);
      }
      if (log.isTraceEnabled()) {
         log.tracef("Lock prepared, not acquired: %s for index %s", lockName, indexName);
      }
      return lock;
   }

   /**
    * {@inheritDoc}
    */
   public void clearLock(String lockName) {
      //Same special care as above for locks named DEF_LOCK_NAME:
      if (DEF_LOCK_NAME.equals(lockName)) {
         defLock.clearLockSuspending();
      }
      else {
         new TransactionalSharedLuceneLock(cache, indexName, lockName, tm).clearLockSuspending();
      }
      if (log.isTraceEnabled()) {
         log.tracef("Removed lock: %s for index %s", lockName, indexName);
      }
   }
   
}
