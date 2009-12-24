package org.infinispan.loaders.cloud;

import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.loaders.bucket.BucketBasedCacheStore;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jclouds.blobstore.BlobMap;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.http.HttpUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A JClouds-based implementation of a {@link org.infinispan.loaders.bucket.BucketBasedCacheStore}. This file store
 * stores stuff in the following format: <tt>http://{cloud-storage-provider}/{bucket}/{bucket_number}.bucket</tt>
 * <p/>
 *
 * @author Adrian Cole
 * @author Manik Surtani
 * @since 4.0
 */
public class CloudCacheStore extends BucketBasedCacheStore {

   private static final Log log = LogFactory.getLog(CloudCacheStore.class);
   private final ThreadLocal<Set<Future<?>>> asyncCommandFutures = new ThreadLocal<Set<Future<?>>>();
   private CloudCacheStoreConfig config;
   private String containerName;
   private BlobStoreContext<?, ?> ctx;
   // TODO use BlobStore and AsyncBlobStore instead - so that on transactional calls we can use the async store and poll futures for completion.
   private BlobMap blobMap;
   private boolean pollFutures;


   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return CloudCacheStoreConfig.class;
   }

   @Override
   public void init(CacheLoaderConfig cfg, Cache cache, Marshaller m) throws CacheLoaderException {
      this.config = (CloudCacheStoreConfig) cfg;
      init(cfg, cache, m, null, null);
   }

   @Override
   public void stop() throws CacheLoaderException {
      super.stop();
   }

   // this overloaded version of init() is primarily for unit testing/extension.

   protected void init(CacheLoaderConfig config, Cache cache, Marshaller m, BlobStoreContext ctx, BlobMap blobMap) throws CacheLoaderException {
      super.init(config, cache, m);
      this.config = (CloudCacheStoreConfig) config;
      this.cache = cache;
      this.marshaller = m;
      this.ctx = ctx;
      this.blobMap = blobMap;
   }


   @SuppressWarnings("unchecked")
   @Override
   public void start() throws CacheLoaderException {
      super.start();
      if (config.getCloudService() == null)
         throw new ConfigurationException("CloudService must be set!");
      if (config.getIdentity() == null)
         throw new ConfigurationException("Identity must be set");
      if (config.getPassword() == null)
         throw new ConfigurationException("Password must be set");
      if (config.getBucketPrefix() == null)
         throw new ConfigurationException("CloudBucket must be set");
      containerName = getThisContainerName();
      ctx = createBlobStoreContext(config);
      BlobStore bs = ctx.getBlobStore();
      if (!bs.containerExists(containerName)) bs.createContainer(containerName);
      blobMap = ctx.createBlobMap(containerName);
      pollFutures = !config.getAsyncStoreConfig().isEnabled();
   }

   private BlobStoreContext<?, ?> createBlobStoreContext(CloudCacheStoreConfig config) throws CacheLoaderException {
      Properties properties = new Properties();
      InputStream is = Util.loadResourceAsStream("jclouds.properties");
      if (is != null) try {
         properties.load(is);
      } catch (IOException e) {
         log.error("Unable to load contents from jclouds.properties", e);
      }
      BlobStoreContextFactory factory = new BlobStoreContextFactory(properties);

      // Need a URI in blobstore://account:key@service/container/path
      // TODO find a better way to create this context!  Unnecessary construction of a URI only for it to be broken up again into components from within JClouds 
      return factory.createContext(HttpUtils.createUri("blobstore://" + config.getIdentity() +
            ":" + config.getPassword() +
            "@" + config.getCloudService() + "/"));
   }

   private String getThisContainerName() {
      return config.getBucketPrefix() + "-" + cache.getName().toLowerCase();
   }

   @SuppressWarnings("unchecked")
   protected Set<InternalCacheEntry> loadAllLockSafe() throws CacheLoaderException {
      Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>();
      for (Blob blob : blobMap.values()) {
         Bucket bucket = readFromBlob(blob);
         if (bucket.removeExpiredEntries()) updateBucket(bucket);
         result.addAll(bucket.getStoredEntries());
      }
      return result;
   }

   protected void fromStreamLockSafe(ObjectInput objectInput) throws CacheLoaderException {
      String source;
      try {
         source = (String) objectInput.readObject();
      } catch (Exception e) {
         throw convertToCacheLoaderException("Error while reading from stream", e);
      }
      if (getThisContainerName().equals(source)) {
         log.info("Attempt to load the same cloud bucket ignored");
      } else {
         // TODO implement stream handling.   What's the JClouds API to "copy" one bucket to another?
      }
   }

   @Override
   protected boolean supportsMultiThreadedPurge() {
      return true;
   }

   protected void toStreamLockSafe(ObjectOutput objectOutput) throws CacheLoaderException {
      try {
         objectOutput.writeObject(getThisContainerName());
      } catch (IOException e) {
         throw convertToCacheLoaderException("Error while writing to stream", e);
      }
   }

   protected void clearLockSafe() throws CacheLoaderException {
      Set<Future<?>> futures = asyncCommandFutures.get();
      if (futures == null) {
         // is a sync call
         blobMap.clear();
      } else {
         // is an async call - invoke clear() on the container asynchronously and store the future in the 'futures' collection
         // TODO make this async!  AsyncBlobStore ?
         blobMap.clear();
      }
   }

   CacheLoaderException convertToCacheLoaderException(String message, Exception caught) {
      return (caught instanceof CacheLoaderException) ? (CacheLoaderException) caught :
            new CacheLoaderException(message, caught);
   }

   @SuppressWarnings("unchecked")
   protected void purgeInternal() throws CacheLoaderException {
      // TODO can expiry data be stored in a blob's metadata?  More efficient purging that way.

      if (!config.isLazyPurgingOnly()) {
         acquireGlobalLock(false);
         try {
            final BlobMap blobMap = ctx.createBlobMap(containerName);
            if (multiThreadedPurge) {
               purgerService.execute(new Runnable() {
                  @Override
                  public void run() {
                     try {
                        for (Blob blob : blobMap.values()) {
                           Bucket bucket = readFromBlob(blob);
                           if (bucket.removeExpiredEntries()) updateBucket(bucket);
                        }
                     } catch (CacheLoaderException e) {
                        log.warn("Problems purging bucket", e);
                     }
                  }
               });
            } else {
               for (Blob blob : blobMap.values()) {
                  Bucket bucket = readFromBlob(blob);
                  if (bucket.removeExpiredEntries()) updateBucket(bucket);
               }
            }
         } finally {
            releaseGlobalLock(false);
         }
      }
   }

   protected Bucket loadBucket(String hash) throws CacheLoaderException {
      return readFromBlob(blobMap.get(hash));
   }


   protected void insertBucket(Bucket bucket) throws CacheLoaderException {
      Blob blob = blobMap.newBlob();
      writeToBlob(blob, bucket);

      Set<Future<?>> futures = asyncCommandFutures.get();
      if (futures == null) {
         // is a sync call
         blobMap.put(getBucketName(bucket), blob);
      } else {
         // is an async call - invoke clear() on the container asynchronously and store the future in the 'futures' collection
         // TODO make this async!  AsyncBlobStore ?
         blobMap.put(getBucketName(bucket), blob);
      }
   }

   @Override
   protected void applyModifications(List<? extends Modification> mods) throws CacheLoaderException {
      Set<Future<?>> futures = new HashSet<Future<?>>(mods.size());
      asyncCommandFutures.set(futures);
      try {
         super.applyModifications(mods);
         if (pollFutures) {
            CacheLoaderException exception = null;
            try {
               for (Future<?> f : asyncCommandFutures.get()) f.get();
            } catch (InterruptedException ie) {
               Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
               if (e.getCause() instanceof CacheLoaderException)
                  exception = (CacheLoaderException) e.getCause();
               else
                  exception = new CacheLoaderException(e.getCause());
            }
            throw exception;
         }
      } finally {
         asyncCommandFutures.remove();
      }
   }

   protected void updateBucket(Bucket bucket) throws CacheLoaderException {
      insertBucket(bucket);
   }

   private void writeToBlob(Blob blob, Bucket bucket) throws CacheLoaderException {
      try {
         blob.setPayload(marshaller.objectToByteBuffer(bucket));
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   private Bucket readFromBlob(Blob blob) throws CacheLoaderException {
      if (blob == null) return null;
      try {
         return (Bucket) marshaller.objectFromInputStream(blob.getContent());
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   private String getBucketName(Bucket bucket) {
      String bucketName = bucket.getBucketName();
      if (bucketName.startsWith("-")) {
         return bucket.getBucketName().replace("-", "A");
      } else {
         return bucket.getBucketName();
      }
   }
}
