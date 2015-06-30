package org.infinispan.lucene.impl;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * Default factory for locks obtained in <code>InfinispanDirectory</code>, this factory produces instances of
 * <code>BaseLuceneLock</code>.
 *
 * @author Sanne Grinovero
 * @author gustavonalle
 * @see BaseLuceneLock
 * @since 4.0
 */
@SuppressWarnings("unchecked")
public class BaseLockFactory extends LockFactory {

   public static final BaseLockFactory INSTANCE = new BaseLockFactory();

   private static final Log log = LogFactory.getLog(BaseLockFactory.class);

   /**
    * {@inheritDoc}
    */
   @Override
   public BaseLuceneLock makeLock(Directory dir, String lockName) {
      if (!(dir instanceof DirectoryLucene)) {
         throw new UnsupportedOperationException("BaseLuceneLock can only be used with DirectoryLucene, got: " + dir);
      }
      DirectoryLucene infinispanDirectory = (DirectoryLucene) dir;
      Cache distLockCache = infinispanDirectory.getDistLockCache();
      String indexName = infinispanDirectory.getIndexName();
      BaseLuceneLock lock = new BaseLuceneLock(distLockCache, indexName, lockName);
      if (log.isTraceEnabled()) {
         log.tracef("Lock prepared, not acquired: %s for index %s", lockName, indexName);
      }
      return lock;
   }

}
