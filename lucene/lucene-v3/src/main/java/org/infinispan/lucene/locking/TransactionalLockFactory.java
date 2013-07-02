package org.infinispan.lucene.locking;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
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
   @Override
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
   @Override
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
