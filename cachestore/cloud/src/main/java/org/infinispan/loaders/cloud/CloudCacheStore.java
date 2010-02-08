package org.infinispan.loaders.cloud;

import com.google.common.collect.ImmutableSet;
import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.loaders.bucket.BucketBasedCacheStore;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.blobstore.BlobMap;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.blobstore.KeyNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.enterprise.config.EnterpriseConfigurationModule;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


/**
 * The CloudCacheStore implementation that utilizes <a href="http://code.google.com/p/jclouds">JClouds</a> to
 * communicate with cloud storage providers such as <a href="http://aws.amazon.com/s3/">Amazon's S3<a>, <a
 * href="http://www.rackspacecloud.com/cloud_hosting_products/files">Rackspace's Cloudfiles</a>, or any other such
 * provider supported by JClouds.
 * <p/>
 * This file store stores stuff in the following format: <tt>http://{cloud-storage-provider}/{bucket}/{bucket_number}.bucket</tt>
 * <p/>
 *
 * @author Manik Surtani
 * @author Adrian Cole
 * @since 4.0
 */
public class CloudCacheStore extends BucketBasedCacheStore {
   private static final Log log = LogFactory.getLog(CloudCacheStore.class);
   private final ThreadLocal<Set<Future<?>>> asyncCommandFutures = new ThreadLocal<Set<Future<?>>>();
   private CloudCacheStoreConfig cfg;
   private String containerName;
   private BlobStoreContext ctx;
   private BlobStore blobStore;
   private AsyncBlobStore asyncBlobStore;
   private boolean pollFutures = false;
   private boolean constructInternalBlobstores = true;

   @Override
   public Class<? extends CacheStoreConfig> getConfigurationClass() {
      return CloudCacheStoreConfig.class;
   }

   private String getThisContainerName() {
      return cfg.getBucketPrefix() + "-" + cache.getName().toLowerCase().replace("_", "").replace(".", "");
   }

   @Override
   protected boolean supportsMultiThreadedPurge() {
      return true;
   }

   @Override
   public void init(CacheLoaderConfig cfg, Cache<?, ?> cache, Marshaller m) throws CacheLoaderException {
      this.cfg = (CloudCacheStoreConfig) cfg;
      init(cfg, cache, m, null, null, null, true);
   }

   public void init(CacheLoaderConfig cfg, Cache<?, ?> cache, Marshaller m, BlobStoreContext ctx,
                    BlobStore blobStore, AsyncBlobStore asyncBlobStore, boolean constructInternalBlobstores) throws CacheLoaderException {
      super.init(cfg, cache, m);
      this.cfg = (CloudCacheStoreConfig) cfg;
      this.cache = cache;
      this.marshaller = m;
      this.ctx = ctx;
      this.blobStore = blobStore;
      this.asyncBlobStore = asyncBlobStore;
      this.constructInternalBlobstores = constructInternalBlobstores;
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      if (constructInternalBlobstores) {
         if (cfg.getCloudService() == null) throw new ConfigurationException("CloudService must be set!");
         if (cfg.getIdentity() == null) throw new ConfigurationException("Identity must be set");
         if (cfg.getPassword() == null) throw new ConfigurationException("Password must be set");
      }
      if (cfg.getBucketPrefix() == null) throw new ConfigurationException("CloudBucket must be set");
      containerName = getThisContainerName();
      try {
         if (constructInternalBlobstores) {
            // add an executor as a constructor param to EnterpriseConfigurationModule, pass property overrides instead of Properties()
            ctx = new BlobStoreContextFactory().createContext(cfg.getCloudService(), cfg.getIdentity(), cfg.getPassword(),
                                                           ImmutableSet.of(new EnterpriseConfigurationModule(), new Log4JLoggingModule()), new Properties());
            blobStore = ctx.getBlobStore();
            asyncBlobStore = ctx.getAsyncBlobStore();
         }

         // the "location" is not currently used.
         if (!blobStore.containerExists(containerName)) blobStore.createContainerInLocation(cfg.getCloudServiceLocation(), containerName);
         pollFutures = !cfg.getAsyncStoreConfig().isEnabled();
      } catch (IOException ioe) {
         throw new CacheLoaderException("Unable to create context", ioe);
      }
   }

   protected Set<InternalCacheEntry> loadAllLockSafe() throws CacheLoaderException {
      Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>();

      for (Map.Entry<String, Blob> entry : ctx.createBlobMap(containerName).entrySet()) {
         Bucket bucket = readFromBlob(entry.getValue(), entry.getKey());
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
      if (containerName.equals(source)) {
         log.info("Attempt to load the same cloud bucket ({0}) ignored", source);
      } else {
         // TODO implement stream handling.   What's the JClouds API to "copy" one bucket to another?
      }
   }

   protected void toStreamLockSafe(ObjectOutput objectOutput) throws CacheLoaderException {
      try {
         objectOutput.writeObject(containerName);
      } catch (Exception e) {
         throw convertToCacheLoaderException("Error while writing to stream", e);
      }
   }

   protected void clearLockSafe() {
      Set<Future<?>> futures = asyncCommandFutures.get();
      if (futures == null) {
         // is a sync call
         blobStore.clearContainer(containerName);
      } else {
         // is an async call - invoke clear() on the container asynchronously and store the future in the 'futures' collection
         futures.add(asyncBlobStore.clearContainer(containerName));
      }
   }

   private CacheLoaderException convertToCacheLoaderException(String m, Throwable c) {
      if (c instanceof CacheLoaderException) {
         return (CacheLoaderException) c;
      } else {
         return new CacheLoaderException(m, c);
      }
   }

   protected Bucket loadBucket(String hash) throws CacheLoaderException {
      try {
         return readFromBlob(blobStore.getBlob(containerName, encodeBucketName(hash)), hash);
      } catch (KeyNotFoundException e) {
         return null;
      }
   }

   private void purge(BlobMap blobMap) throws CacheLoaderException {
      for (Map.Entry<String, Blob> entry : blobMap.entrySet()) {
         Bucket bucket = readFromBlob(entry.getValue(), entry.getKey());
         if (bucket.removeExpiredEntries()) updateBucket(bucket);
      }
   }

   protected void purgeInternal() throws CacheLoaderException {
      // TODO can expiry data be stored in a blob's metadata?  More efficient purging that way.  See https://jira.jboss.org/jira/browse/ISPN-334
      if (!cfg.isLazyPurgingOnly()) {
         acquireGlobalLock(false);
         try {
            final BlobMap blobMap = ctx.createBlobMap(containerName);
            if (multiThreadedPurge) {
               purgerService.execute(new Runnable() {
                  public void run() {
                     try {
                        purge(blobMap);
                     } catch (Exception e) {
                        log.warn("Problems purging", e);
                     }
                  }
               });
            } else {
               purge(blobMap);
            }
         } finally {
            releaseGlobalLock(false);
         }
      }
   }

   protected void insertBucket(Bucket bucket) throws CacheLoaderException {
      Blob blob = blobStore.newBlob(encodeBucketName(bucket.getBucketName()));
      writeToBlob(blob, bucket);

      Set<Future<?>> futures = asyncCommandFutures.get();
      if (futures == null) {
         // is a sync call
         blobStore.putBlob(containerName, blob);
      } else {
         // is an async call - invoke clear() on the container asynchronously and store the future in the 'futures' collection
         futures.add(asyncBlobStore.putBlob(containerName, blob));
      }
   }

   @Override
   public void applyModifications(List<? extends Modification> modifications) throws CacheLoaderException {
      Set<Future<?>> futures = new HashSet<Future<?>>();
      asyncCommandFutures.set(futures);

      try {
         super.applyModifications(modifications);
         if (pollFutures) {
            CacheLoaderException exception = null;
            try {
               for (Future<?> f : asyncCommandFutures.get()) f.get();
            } catch (InterruptedException ie) {
               Thread.currentThread().interrupt();
            } catch (ExecutionException ee) {
               exception = convertToCacheLoaderException("Caught exception in async process", ee.getCause());
            }
            if (exception != null) throw exception;
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
         blob.setPayload(
               marshaller.objectToByteBuffer(bucket));
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   private Bucket readFromBlob(Blob blob, String bucketName) throws CacheLoaderException {
      if (blob == null) return null;
      try {
         Bucket bucket = (Bucket) marshaller.objectFromInputStream(blob.getContent());
         if (bucket != null) bucket.setBucketName(bucketName);
         return bucket;
      } catch (Exception e) {
         throw convertToCacheLoaderException("Unable to read blob", e);
      }
   }

   private String encodeBucketName(String decodedName) {
      return (decodedName.startsWith("-")) ?  
            decodedName.replace('-', 'A') :
            decodedName;
   }
}
