package org.infinispan.lucene.impl;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.lucene.directory.BuildContext;
import org.infinispan.lucene.locking.BaseLockFactory;
import org.infinispan.lucene.logging.Log;
import org.infinispan.lucene.readlocks.DistributedSegmentReadLocker;
import org.infinispan.lucene.readlocks.SegmentReadLocker;
import org.infinispan.util.logging.LogFactory;

public class DirectoryBuilderImpl implements BuildContext {

   private static final Log log = LogFactory.getLog(DirectoryBuilderImpl.class, Log.class);

   /**
    * Used as default chunk size: each Lucene index segment is split into smaller parts having a default size in bytes as
    * defined here
    */
   public final static int DEFAULT_BUFFER_SIZE = 16 * 1024;

   /**
    * Mandatory parameters:
    */

   private final Cache<?, ?> metadataCache;
   private final Cache<?, ?> chunksCache;
   private final Cache<?, ?> distLocksCache;
   private final String indexName;

   /**
    * Optional parameters:
    */

   private int chunkSize = DEFAULT_BUFFER_SIZE;
   private SegmentReadLocker srl = null;
   private LockFactory lockFactory = null;

   public DirectoryBuilderImpl(Cache<?, ?> metadataCache, Cache<?, ?> chunksCache, Cache<?, ?> distLocksCache, String indexName) {
      this.metadataCache = checkValidConfiguration(checkNotNull(metadataCache, "metadataCache"), indexName);
      this.chunksCache = checkValidConfiguration(checkNotNull(chunksCache, "chunksCache"), indexName);
      this.distLocksCache = checkValidConfiguration(checkNotNull(distLocksCache, "distLocksCache"), indexName);
      this.indexName =  checkNotNull(indexName, "indexName");
      validateMetadataCache(metadataCache, indexName);
   }

   @Override
   public Directory create() {
      if (lockFactory == null) {
         lockFactory = makeDefaultLockFactory(distLocksCache, indexName);
      }
      if (srl == null) {
         srl = makeDefaultSegmentReadLocker(metadataCache, chunksCache, distLocksCache, indexName);
      }
      if (LuceneVersionDetector.VERSION == 3) {
         return new DirectoryLuceneV3(metadataCache, chunksCache, indexName, lockFactory, chunkSize, srl);
      }
      else {
         Class<?>[] ctorType = new Class[]{ Cache.class, Cache.class, String.class, LockFactory.class, int.class, SegmentReadLocker.class };
         Directory d;
         try {
            d = (Directory) DirectoryBuilderImpl.class.getClassLoader()
               .loadClass("org.infinispan.lucene.impl.DirectoryLuceneV4")
               .getConstructor(ctorType)
               .newInstance(metadataCache, chunksCache, indexName, lockFactory, chunkSize, srl);
         } catch (Exception e) {
            throw log.failedToCreateLucene4Directory(e);
         }
         return d;
      }
   }

   @Override
   public BuildContext chunkSize(int bytes) {
      if (bytes <= 0)
         throw new IllegalArgumentException("chunkSize must be a positive integer");
      this.chunkSize = bytes;
      return this;
   }

   @Override
   public BuildContext overrideSegmentReadLocker(SegmentReadLocker srl) {
      checkNotNull(srl, "srl");
      this.srl = srl;
      return this;
   }

   @Override
   public BuildContext overrideWriteLocker(LockFactory lockFactory) {
      checkNotNull(lockFactory, "lockFactory");
      this.lockFactory = lockFactory;
      return this;
   }

   private static SegmentReadLocker makeDefaultSegmentReadLocker(Cache<?, ?> metadataCache, Cache<?, ?> chunksCache, Cache<?, ?> distLocksCache, String indexName) {
      checkNotNull(distLocksCache, "distLocksCache");
      checkNotNull(indexName, "indexName");
      return new DistributedSegmentReadLocker((Cache<Object, Integer>) distLocksCache, chunksCache, metadataCache, indexName);
   }

   private static <T> T checkNotNull(final T v,final String objectname) {
      if (v == null)
         throw log.requiredParameterWasPassedNull(objectname);
      return v;
   }

   private static Cache<?, ?> checkValidConfiguration(final Cache<?, ?> cache, String indexName) {
      if (cache == null) {
         return null;
      }
      Configuration configuration = cache.getCacheConfiguration();
      if (configuration.expiration().maxIdle() != -1) {
         throw log.luceneStorageHavingIdleTimeSet( indexName, cache.getName() );
      }
      if (configuration.expiration().lifespan() != -1) {
         throw log.luceneStorageHavingLifespanSet( indexName, cache.getName() );
      }
      if (configuration.storeAsBinary().enabled()) {
         throw log.luceneStorageAsBinaryEnabled(indexName, cache.getName());
      }
      if (!Configurations.noDataLossOnJoiner(configuration)) {
         throw log.luceneStorageNoStateTransferEnabled(indexName, cache.getName());
      }
      return cache;
   }

   private static LockFactory makeDefaultLockFactory(Cache<?, ?> cache, String indexName) {
      checkNotNull(cache, "cache");
      checkNotNull(indexName, "indexName");
      return new BaseLockFactory(cache, indexName);
   }

   private static void validateMetadataCache(Cache<?, ?> cache, String indexName) {
      Configuration configuration = cache.getCacheConfiguration();
      if (configuration.eviction().strategy().isEnabled()) {
         throw log.evictionNotAllowedInMetadataCache(indexName, cache.getName());
      }
      if (configuration.persistence().usingStores() && !configuration.persistence().preload()) {
         throw log.preloadNeededIfPersistenceIsEnabledForMetadataCache(indexName, cache.getName());
      }
   }

}
