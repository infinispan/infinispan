package org.infinispan.lucene.locking;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Default factory for locks obtained in <code>InfinispanDirectory</code>,
 * this factory produces instances of <code>BaseLuceneLock</code>.
 *
 * @since 4.0
 * @author Sanne Grinovero
 * @see org.infinispan.lucene.locking.BaseLuceneLock
 */
@SuppressWarnings("unchecked")
public class BaseLockFactory extends LockFactory {

   static final String DEF_LOCK_NAME = IndexWriter.WRITE_LOCK_NAME;
   private static final Log log = LogFactory.getLog(BaseLockFactory.class);

   private final Cache<?, ?> cache;
   private final String indexName;
   private final BaseLuceneLock defLock;

   public BaseLockFactory(Cache<?, ?> cache, String indexName) {
      this.cache = cache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING);
      this.indexName = indexName;
      defLock = new BaseLuceneLock(this.cache, indexName, DEF_LOCK_NAME);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public BaseLuceneLock makeLock(String lockName) {
      BaseLuceneLock lock;
      //It appears Lucene always uses the same name so we give locks
      //having this name a special treatment:
      if (DEF_LOCK_NAME.equals(lockName)) {
         lock = defLock;
      }
      else {
         // this branch is never taken with current Lucene version.
         lock = new BaseLuceneLock(cache, indexName, lockName);
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
         defLock.clearLock();
      }
      else {
         new BaseLuceneLock(cache, indexName, lockName).clearLock();
      }
      if (log.isTraceEnabled()) {
         log.tracef("Removed lock: %s for index %s", lockName, indexName);
      }
   }

}
