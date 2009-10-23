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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.TransactionManager;
import java.io.IOException;

/**
 * Factory for locks obtained in <code>InfinispanDirectory</code>
 * 
 * @author Sanne Grinovero
 * @since 4.0
 * @author Lukasz Moren
 * @see org.infinispan.lucene.InfinispanDirectory
 * @see org.infinispan.lucene.InfinispanLockFactory.InfinispanLock
 */
public class InfinispanLockFactory extends LockFactory {

   private static final Log log = LogFactory.getLog(InfinispanLockFactory.class);
   static final String DEF_LOCK_NAME = IndexWriter.WRITE_LOCK_NAME;

   private final Cache<CacheKey, Object> cache;
   private final String indexName;
   private final TransactionManager tm;
   private final InfinispanLock defLock;

   public InfinispanLockFactory(Cache<CacheKey, Object> cache, String indexName) {
      this.cache = cache;
      this.indexName = indexName;
//      tm = cache.getAdvancedCache().getComponentRegistry().getComponent(TransactionManager.class);
//      if (tm == null) {
//         throw new CacheException(
//                  "Failed looking up TransactionManager, check if any transaction manager is associated with Infinispan cache: "
//                           + cache.getName());
//      }
      tm = null;
      defLock = new InfinispanLock(cache, indexName, DEF_LOCK_NAME, tm);
   }

   /**
    * {@inheritDoc}
    */
   public InfinispanLock makeLock(String lockName) {
      InfinispanLock lock;
      //It appears Lucene always uses the same name so we give locks
      //having this name a special treatment:
      if (DEF_LOCK_NAME.equals(lockName)) {
         lock = defLock;
      }
      else {
         // this branch is never taken with current Lucene version.
         lock = new InfinispanLock(cache, indexName, lockName, tm);
      }
      if (log.isTraceEnabled()) {
         log.trace("Lock prepared, not acquired: {0} for index {1}", lockName, indexName);
      }
      return lock;
   }

   /**
    * {@inheritDoc}
    */
   public void clearLock(String lockName) throws IOException {
      //Same special care as above for locks named DEF_LOCK_NAME:
      if (DEF_LOCK_NAME.equals(lockName)) {
         defLock.clearLock();
      }
      else {
         new InfinispanLock(cache, indexName, lockName, tm).clearLock();
      }
      if (log.isTraceEnabled()) {
         log.trace("Removed lock: {0} for index {1}", lockName, indexName);
      }
   }
   
}
