package org.infinispan.lucene.locking;

import java.io.IOException;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.apache.lucene.store.Lock;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.context.Flag;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Inter-IndexWriter Lucene index lock based on Infinispan.
 * There are pros and cons about using this implementation, please see javadoc on
 * the factory class <code>org.infinispan.lucene.locking.TransactionalLockFactory</code>
 *
 * @since 4.0
 * @author Sanne Grinovero
 * @see org.infinispan.lucene.locking.TransactionalLockFactory
 * @see org.apache.lucene.store.Lock
 */
@SuppressWarnings("unchecked")
class TransactionalSharedLuceneLock extends Lock {

   private static final Log log = LogFactory.getLog(TransactionalSharedLuceneLock.class, Log.class);

   private final Cache<FileCacheKey, FileCacheKey> noCacheStoreCache;
   private final String lockName;
   private final String indexName;
   private final TransactionManager tm;
   private final FileCacheKey keyOfLock;

   TransactionalSharedLuceneLock(Cache<?, ?> cache, String indexName, String lockName, TransactionManager tm) {
      this.noCacheStoreCache = (Cache<FileCacheKey, FileCacheKey>) cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE, Flag.SKIP_CACHE_LOAD, Flag.SKIP_INDEXING);
      this.lockName = lockName;
      this.indexName = indexName;
      this.tm = tm;
      this.keyOfLock = new FileCacheKey(indexName, lockName);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean obtain() throws IOException {
      Object previousValue = noCacheStoreCache.putIfAbsent(keyOfLock, keyOfLock);
      if (previousValue == null) {
         if (log.isTraceEnabled()) {
            log.tracef("Lock: %s acquired for index: %s", lockName, indexName);
         }
         // we own the lock:
         startTransaction();
         return true;
      }
      else {
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
   public void release() throws IOException {
      try {
         commitTransactions();
      }
      finally {
         clearLock();
      }
   }

   /**
    * Removes the lock, without committing pending changes or involving transactions. Used by Lucene
    * at Directory creation: we expect the lock to not exist in this case.
    */
   private void clearLock() {
      Object previousValue = noCacheStoreCache.remove(keyOfLock);
      if (previousValue!=null && log.isTraceEnabled()) {
         log.tracef("Lock removed for index: %s", indexName);
      }
   }

   @Override
   public boolean isLocked() {
      boolean locked = false;
      Transaction tx = null;
      try {
         // if there is an ongoing transaction we need to suspend it
         if ((tx = tm.getTransaction()) != null) {
            tm.suspend();
         }
         locked = noCacheStoreCache.containsKey(keyOfLock);
      }
      catch (Exception e) {
         log.errorSuspendingTransaction(e);
      }
      finally {
         if (tx != null) {
            try {
               tm.resume(tx);
            } catch (Exception e) {
               throw new CacheException("Unable to resume suspended transaction " + tx, e);
            }
         }
      }
      return locked;
   }

   /**
    * Starts a new transaction. Used to batch changes in LuceneDirectory:
    * a transaction is created at lock acquire, and closed on release.
    * It's also committed and started again at each IndexWriter.commit();
    *
    * @throws IOException wraps Infinispan exceptions to adapt to Lucene's API
    */
   private void startTransaction() throws IOException {
      try {
         tm.begin();
      }
      catch (Exception e) {
         log.unableToStartTransaction(e);
         throw new IOException("SharedLuceneLock could not start a transaction after having acquired the lock", e);
      }
      if (log.isTraceEnabled()) {
         log.tracef("Batch transaction started for index: %s", indexName);
      }
   }

   /**
    * Commits the existing transaction.
    * It's illegal to call this if a transaction was not started.
    *
    * @throws IOException wraps Infinispan exceptions to adapt to Lucene's API
    */
   private void commitTransactions() throws IOException {
      try {
         tm.commit();
      }
      catch (Exception e) {
         log.unableToCommitTransaction(e);
         throw new IOException("SharedLuceneLock could not commit a transaction", e);
      }
      if (log.isTraceEnabled()) {
         log.tracef("Batch transaction commited for index: %s", indexName);
      }
   }

   /**
    * Will clear the lock, eventually suspending a running transaction to make sure the
    * release is immediately taking effect.
    */
   public void clearLockSuspending() {
      Transaction tx = null;
      try {
         // if there is an ongoing transaction we need to suspend it
         if ((tx = tm.getTransaction()) != null) {
            tm.suspend();
         }
         clearLock();
      }
      catch (Exception e) {
         log.errorSuspendingTransaction(e);
      }
      finally {
         if (tx != null) {
            try {
               tm.resume(tx);
            } catch (Exception e) {
               throw new CacheException("Unable to resume suspended transaction " + tx, e);
            }
         }
      }
   }

}
