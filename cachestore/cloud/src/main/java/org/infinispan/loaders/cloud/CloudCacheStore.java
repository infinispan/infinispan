package org.infinispan.loaders.cloud;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.loaders.bucket.BucketBasedCacheStore;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.util.stream.Streams;
import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.domain.Location;
import org.jclouds.enterprise.config.EnterpriseConfigurationModule;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;

import com.google.common.collect.ImmutableSet;

/**
 * The CloudCacheStore implementation that utilizes <a
 * href="http://code.google.com/p/jclouds">JClouds</a> to communicate with cloud storage providers
 * such as <a href="http://aws.amazon.com/s3/">Amazon's S3<a>, <a
 * href="http://www.rackspacecloud.com/cloud_hosting_products/files">Rackspace's Cloudfiles</a>, or
 * any other such provider supported by JClouds.
 * <p/>
 * This file store stores stuff in the following format:
 * <tt>http://{cloud-storage-provider}/{bucket}/{bucket_number}.bucket</tt>
 * <p/>
 *
 * @author Manik Surtani
 * @author Adrian Cole
 * @since 4.0
 */
@CacheLoaderMetadata(configurationClass = CloudCacheStoreConfig.class)
public class CloudCacheStore extends BucketBasedCacheStore {
   static final int COMPRESSION_COPY_BYTEARRAY_SIZE = 1024;
   private static final Log log = LogFactory.getLog(CloudCacheStore.class);
   final ThreadLocal<List<Future<?>>> asyncCommandFutures = new ThreadLocal<List<Future<?>>>();
   CloudCacheStoreConfig cfg;
   String containerName;
   BlobStoreContext ctx;
   BlobStore blobStore;
   AsyncBlobStore asyncBlobStore;
   boolean pollFutures = false;
   boolean constructInternalBlobstores = true;
   protected static final String EARLIEST_EXPIRY_TIME = "metadata_eet";

   @Override
   public Class<? extends CacheStoreConfig> getConfigurationClass() {
      return CloudCacheStoreConfig.class;
   }

   private String getThisContainerName() {
      return cfg.getBucketPrefix() + "-"
              + cache.getName().toLowerCase().replace("_", "").replace(".", "");
   }

   @Override
   protected boolean supportsMultiThreadedPurge() {
      return true;
   }

   @Override
   public void init(CacheLoaderConfig cfg, Cache<?, ?> cache, Marshaller m)
           throws CacheLoaderException {
      this.cfg = (CloudCacheStoreConfig) cfg;
      init(cfg, cache, m, null, null, null, true);
   }

   public void init(CacheLoaderConfig cfg, Cache<?, ?> cache, Marshaller m, BlobStoreContext ctx,
                    BlobStore blobStore, AsyncBlobStore asyncBlobStore, boolean constructInternalBlobstores)
           throws CacheLoaderException {
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
         if (cfg.getCloudService() == null)
            throw new ConfigurationException("CloudService must be set!");
         if (cfg.getIdentity() == null)
            throw new ConfigurationException("Identity must be set");
         if (cfg.getPassword() == null)
            throw new ConfigurationException("Password must be set");
      }
      if (cfg.getBucketPrefix() == null)
         throw new ConfigurationException("CloudBucket must be set");
      containerName = getThisContainerName();
      try {
         if (constructInternalBlobstores) {
            // add an executor as a constructor param to
            // EnterpriseConfigurationModule, pass
            // property overrides instead of Properties()
            ctx = new BlobStoreContextFactory().createContext(cfg.getCloudService(), cfg
                    .getIdentity(), cfg.getPassword(), ImmutableSet.of(
                    new EnterpriseConfigurationModule(), new Log4JLoggingModule()),
                    new Properties());
            blobStore = ctx.getBlobStore();
            asyncBlobStore = ctx.getAsyncBlobStore();
         }

         // the "location" is not currently used.
         if (!blobStore.containerExists(containerName)) {
            Location chosenLoc = null;
            if (cfg.getCloudServiceLocation() != null && cfg.getCloudServiceLocation().trim().length() > 0) {
               Map<String, ? extends Location> availableLocations = blobStore.getLocations();
               String loc = cfg.getCloudServiceLocation().trim().toLowerCase();
               chosenLoc = availableLocations.get(loc);
               if (chosenLoc == null) {
                  log.warn(
                     String.format("Unable to use configured Cloud Service Location [%s].  Available locations for Cloud Service [%s] are %s",
                     loc, cfg.getCloudService(), availableLocations.keySet()));
               }
            }
            blobStore.createContainerInLocation(chosenLoc, containerName);
         }
         pollFutures = !cfg.getAsyncStoreConfig().isEnabled();
      } catch (IOException ioe) {
         throw new CacheLoaderException("Unable to create context", ioe);
      }
   }

   @Override
   protected void loopOverBuckets(BucketHandler handler) throws CacheLoaderException {
      for (Map.Entry<String, Blob> entry : ctx.createBlobMap(containerName).entrySet()) {
         Bucket bucket = readFromBlob(entry.getValue(), entry.getKey());
         if (bucket.removeExpiredEntries())
            updateBucket(bucket);
         if (handler.handle(bucket))
            break;
      }
   }

   @Override
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
         // TODO implement stream handling. What's the JClouds API to "copy" one bucket to another?
      }
   }

   @Override
   protected void toStreamLockSafe(ObjectOutput objectOutput) throws CacheLoaderException {
      try {
         objectOutput.writeObject(containerName);
      } catch (Exception e) {
         throw convertToCacheLoaderException("Error while writing to stream", e);
      }
   }

   @Override
   protected void clearLockSafe() {
      List<Future<?>> futures = asyncCommandFutures.get();
      if (futures == null) {
         // is a sync call
         blobStore.clearContainer(containerName);
      } else {
         // is an async call - invoke clear() on the container asynchronously
         // and store the future
         // in the 'futures' collection
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

   @Override
   protected Bucket loadBucket(String hash) throws CacheLoaderException {
      return readFromBlob(blobStore.getBlob(containerName, encodeBucketName(hash)), hash);
   }

   private void purge() throws CacheLoaderException {
      long currentTime = System.currentTimeMillis();
      PageSet<? extends StorageMetadata> ps = blobStore.list(containerName, ListContainerOptions.Builder.withDetails());

      for (StorageMetadata sm : ps) {
         long lastExpirableEntry = readLastExpirableEntryFromMetadata(sm.getUserMetadata());
         if (lastExpirableEntry < currentTime)
            scanBlobForExpiredEntries(sm.getName());
      }
   }

   private void scanBlobForExpiredEntries(String blobName) {
      Blob blob = blobStore.getBlob(containerName, blobName);
      try {
         Bucket bucket = readFromBlob(blob, blobName);
         if (bucket.removeExpiredEntries())
            updateBucket(bucket);
      } catch (CacheLoaderException e) {
         log.warn("Unable to read blob at {0}", blobName, e);
      }
   }

   private long readLastExpirableEntryFromMetadata(Map<String, String> metadata) {
      String eet = metadata.get(EARLIEST_EXPIRY_TIME);
      long eetLong = -1;
      if (eet != null)
         eetLong = Long.parseLong(eet);
      return eetLong;
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      if (!cfg.isLazyPurgingOnly()) {
         acquireGlobalLock(false);
         try {
            if (multiThreadedPurge) {
               purgerService.execute(new Runnable() {
                  public void run() {
                     try {
                        purge();
                     } catch (Exception e) {
                        log.warn("Problems purging", e);
                     }
                  }
               });
            } else {
               purge();
            }
         } finally {
            releaseGlobalLock(false);
         }
      }
   }

   @Override
   protected void updateBucket(Bucket bucket) throws CacheLoaderException {
      Blob blob = blobStore.newBlob(encodeBucketName(bucket.getBucketName()));
      writeToBlob(blob, bucket);

      List<Future<?>> futures = asyncCommandFutures.get();
      if (futures == null) {
         // is a sync call
         blobStore.putBlob(containerName, blob);
      } else {
         // is an async call - invoke clear() on the container asynchronously
         // and store the future
         // in the 'futures' collection
         futures.add(asyncBlobStore.putBlob(containerName, blob));
      }
   }

   @Override
   public void applyModifications(List<? extends Modification> modifications)
           throws CacheLoaderException {
      List<Future<?>> futures = new LinkedList<Future<?>>();
      asyncCommandFutures.set(futures);

      try {
         super.applyModifications(modifications);
         if (pollFutures) {
            CacheLoaderException exception = null;
            try {
               futures = asyncCommandFutures.get();
               if (log.isTraceEnabled())
                  log.trace("Futures, in order: {0}", futures);
               for (Future<?> f : futures) {
                  Object o = f.get();
                  if (log.isTraceEnabled())
                     log.trace("Future {0} returned {1}", f, o);
               }
            } catch (InterruptedException ie) {
               Thread.currentThread().interrupt();
            } catch (ExecutionException ee) {
               exception = convertToCacheLoaderException("Caught exception in async process", ee
                       .getCause());
            }
            if (exception != null)
               throw exception;
         }
      } finally {
         asyncCommandFutures.remove();
      }
   }

   private void writeToBlob(Blob blob, Bucket bucket) throws CacheLoaderException {
      long earliestExpiryTime = -1;
      for (InternalCacheEntry e : bucket.getEntries().values()) {
         long t = e.getExpiryTime();
         if (t != -1) {
            if (earliestExpiryTime == -1)
               earliestExpiryTime = t;
            else
               earliestExpiryTime = Math.min(earliestExpiryTime, t);
         }
      }

      try {
         final byte[] payloadBuffer = marshaller.objectToByteBuffer(bucket);
         if (cfg.isCompress())
            blob.setPayload(compress(payloadBuffer));
         else
            blob.setPayload(payloadBuffer);
         if (earliestExpiryTime > -1) {
            Map<String, String> md = Collections.singletonMap(EARLIEST_EXPIRY_TIME, String
                    .valueOf(earliestExpiryTime));
            blob.getMetadata().setUserMetadata(md);
         }

      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   private Bucket readFromBlob(Blob blob, String bucketName) throws CacheLoaderException {
      if (blob == null)
         return null;
      BZip2CompressorInputStream is = null;
      try {
         Bucket bucket;
         final InputStream content = blob.getContent();
         if (cfg.isCompress()) {
            // TODO re-enable streaming
            // everything is copied in memory in order to avoid an unfixed UnexpectedEOF exception
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            Streams.copy(content, bos);
            final byte[] byteArray = bos.toByteArray();
            ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
            is = new BZip2CompressorInputStream(bis);
            ByteArrayOutputStream bos2 = new ByteArrayOutputStream();
            try {
               Streams.copy(is, bos2);
               final byte[] byteArray2 = bos2.toByteArray();

               log.debug("Decompressed " + bucketName + " from " + byteArray.length + " -> "
                       + byteArray2.length);

               bucket = (Bucket) marshaller.objectFromInputStream(new ByteArrayInputStream(
                       byteArray2));
            } finally {
               is.close();
               bis.close();
               bos.close();
               bos2.close();
            }
         } else
            bucket = (Bucket) marshaller.objectFromInputStream(content);

         if (bucket != null)
            bucket.setBucketName(bucketName);
         return bucket;
      } catch (ClassNotFoundException e) {
         throw convertToCacheLoaderException("Unable to read blob", e);
      } catch (IOException e) {
         throw convertToCacheLoaderException("Class loading issue", e);
      }
   }

   private byte[] compress(final byte[] payloadBuffer) throws IOException {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      InputStream input = new ByteArrayInputStream(payloadBuffer);
      BZip2CompressorOutputStream output = new BZip2CompressorOutputStream(baos);
      try {
         Streams.copy(input, output);
         return baos.toByteArray();
      } finally {
         output.close();
         input.close();
         baos.close();
      }
   }

   private String encodeBucketName(String decodedName) {
      final String name = (decodedName.startsWith("-")) ? decodedName.replace('-', 'A')
              : decodedName;
      if (cfg.isCompress())
         return name + ".bz2";
      return name;
   }
}
