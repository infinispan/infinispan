package org.infinispan.lucene.impl;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;

/**
 * Default factory for locks obtained in <code>InfinispanDirectory</code>, this factory produces instances of
 * <code>BaseLuceneLock</code>.
 *
 * @author Sanne Grinovero
 * @author gustavonalle
 * @see BaseLuceneLock
 * @since 4.0
 */
public class BaseLockFactory extends LockFactory {

   public static final BaseLockFactory INSTANCE = new BaseLockFactory();

   /**
    * {@inheritDoc}
    */
   @Override
   public BaseLuceneLock obtainLock(Directory dir, String lockName) throws IOException {
      if (!(dir instanceof DirectoryLucene)) {
         throw new UnsupportedOperationException("BaseLuceneLock can only be used with DirectoryLucene, got: " + dir);
      }
      DirectoryLucene infinispanDirectory = (DirectoryLucene) dir;
      int affinitySegmentId = infinispanDirectory.getAffinitySegmentId();
      Cache distLockCache = infinispanDirectory.getDistLockCache();
      String indexName = infinispanDirectory.getIndexName();
      BaseLuceneLock lock = new BaseLuceneLock(distLockCache, indexName, lockName, affinitySegmentId);
      CommonLockObtainUtils.attemptObtain(lock);
      return lock;
   }

}
