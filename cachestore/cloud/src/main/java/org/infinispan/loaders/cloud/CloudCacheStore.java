/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
package org.infinispan.loaders.cloud;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
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
import org.infinispan.loaders.cloud.logging.Log;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.stream.Streams;
import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.domain.Location;
import org.jclouds.enterprise.config.EnterpriseConfigurationModule;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

/**
 * The CloudCacheStore implementation that utilizes <a href="http://code.google.com/p/jclouds">JClouds</a> to
 * communicate with cloud storage providers such as <a href="http://aws.amazon.com/s3/">Amazon's S3<a>, <a
 * href="http://www.rackspacecloud.com/cloud_hosting_products/files">Rackspace's Cloudfiles</a>, or any other such
 * provider supported by JClouds.
 * <p/>
 * This file store stores stuff in the following format: <tt>http://{cloud-storage-provider}/{bucket}/{bucket_number}</tt>
 * <p/>
 *
 * @author Manik Surtani
 * @author Adrian Cole
 * @since 4.0
 */
@CacheLoaderMetadata(configurationClass = CloudCacheStoreConfig.class)
public class CloudCacheStore extends BucketBasedCacheStore {
   static final Log log = LogFactory.getLog(CloudCacheStore.class, Log.class);
   final ThreadLocal<List<Future<?>>> asyncCommandFutures = new ThreadLocal<List<Future<?>>>();
   CloudCacheStoreConfig cfg;
   String containerName;
   BlobStoreContext ctx;
   BlobStore blobStore;
   AsyncBlobStore asyncBlobStore;
   boolean pollFutures = false;
   boolean constructInternalBlobstores = true;
   protected static final String EARLIEST_EXPIRY_TIME = "metadata_eet";
   private MessageDigest md5;

   public CloudCacheStore() {
      try {
         md5 = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException ignore) {
         md5 = null;
      }
   }

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
   public void init(CacheLoaderConfig cfg, Cache<?, ?> cache, StreamingMarshaller m)
         throws CacheLoaderException {
      this.cfg = (CloudCacheStoreConfig) cfg;
      init(cfg, cache, m, null, null, null, true);
   }

   public void init(CacheLoaderConfig cfg, Cache<?, ?> cache, StreamingMarshaller m, BlobStoreContext ctx,
                    BlobStore blobStore, AsyncBlobStore asyncBlobStore, boolean constructInternalBlobstores)
         throws CacheLoaderException {
      super.init(cfg, cache, m);
      this.cfg = (CloudCacheStoreConfig) cfg;
      marshaller = m;
      this.ctx = ctx;
      this.blobStore = blobStore;
      this.asyncBlobStore = asyncBlobStore;
      this.constructInternalBlobstores = constructInternalBlobstores;
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      if (constructInternalBlobstores) {
         if (cfg.getCloudService() == null) {
            throw new ConfigurationException("CloudService must be set!");
        }
         if (cfg.getIdentity() == null) {
            throw new ConfigurationException("Identity must be set");
        }
         if (cfg.getPassword() == null) {
            throw new ConfigurationException("Password must be set");
        }
      }
      if (cfg.getBucketPrefix() == null) {
        throw new ConfigurationException("CloudBucket must be set");
    }
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

         if (!blobStore.containerExists(containerName)) {
            Location chosenLoc = null;
            if (cfg.getCloudServiceLocation() != null && cfg.getCloudServiceLocation().trim().length() > 0) {
               Map<String, ? extends Location> idToLocation = Maps.uniqueIndex(blobStore.listAssignableLocations(), new Function<Location, String>() {
                  @Override
                  public String apply(Location input) {
                     return input.getId();
                  }
               });
               String loc = cfg.getCloudServiceLocation().trim().toLowerCase();
               chosenLoc = idToLocation.get(loc);
               if (chosenLoc == null) {
                  log.unableToConfigureCloudService(loc, cfg.getCloudService(), idToLocation.keySet());
               }
            }
            blobStore.createContainerInLocation(chosenLoc, containerName);
         }
         pollFutures = !cfg.getAsyncStoreConfig().isEnabled();
      } catch (RuntimeException ioe) {
         throw new CacheLoaderException("Unable to create context", ioe);
      }
   }


   @Override
   protected void loopOverBuckets(BucketHandler handler) throws CacheLoaderException {
      for (Map.Entry<String, Blob> entry : ctx.createBlobMap(containerName).entrySet()) {
         Bucket bucket = readFromBlob(entry.getValue(), entry.getKey());
         if (bucket != null) {
            if (bucket.removeExpiredEntries()) {
               upgradeLock(bucket.getBucketId());
               try {
                  updateBucket(bucket);
               } finally {
                  downgradeLock(bucket.getBucketId());
               }
            }
            if (handler.handle(bucket)) {
               break;
            }
         } else {
            throw new CacheLoaderException("Blob not found: " + entry.getKey());
         }
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
         log.attemptToLoadSameBucketIgnored(source);
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
   protected Bucket loadBucket(Integer hash) throws CacheLoaderException {
      if (hash == null) {
         throw new NullPointerException("hash");
      }
      String bucketName = hash.toString();
      return readFromBlob(blobStore.getBlob(
                  containerName, encodeBucketName(bucketName)), bucketName);
   }

   void purge() {
      long currentTime = timeService.wallClockTime();
      PageSet<? extends StorageMetadata> ps = blobStore.list(containerName);

      // TODO do we need to scroll through the PageSet?
      for (StorageMetadata sm : ps) {
         long lastExpirableEntry = readLastExpirableEntryFromMetadata(sm.getUserMetadata());
         if (lastExpirableEntry < currentTime) {
            scanBlobForExpiredEntries(sm.getName());
        }
      }
   }

   private void scanBlobForExpiredEntries(String blobName) {
      Blob blob = blobStore.getBlob(containerName, blobName);
      try {
         Bucket bucket = readFromBlob(blob, blobName);
         if (bucket != null) {
            if (bucket.removeExpiredEntries()) {
               upgradeLock(bucket.getBucketId());
               try {
                  updateBucket(bucket);
               } finally {
                  downgradeLock(bucket.getBucketId());
               }
           }
         } else {
            throw new CacheLoaderException("Blob not found: " + blobName);
         }
      } catch (CacheLoaderException e) {
         log.unableToReadBlob(blobName, e);
      }
   }

   private long readLastExpirableEntryFromMetadata(Map<String, String> metadata) {
      String eet = metadata.get(EARLIEST_EXPIRY_TIME);
      long eetLong = -1;
      if (eet != null) {
        eetLong = Long.parseLong(eet);
    }
      return eetLong;
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      if (!cfg.isLazyPurgingOnly()) {
         boolean success = acquireGlobalLock(false);
         try {
            if (multiThreadedPurge) {
               purgerService.execute(new Runnable() {
                  @Override
                  public void run() {
                     try {
                        purge();
                     } catch (Exception e) {
                        log.problemsPurging(e);
                     }
                  }
               });
            } else {
               purge();
            }
         } finally {
            if(success){
               releaseGlobalLock(false);
            }
         }
      }
   }

   @Override
   protected void updateBucket(Bucket bucket) throws CacheLoaderException {
      BlobBuilder builder = blobStore.blobBuilder(encodeBucketName(bucket.getBucketIdAsString()));
      Blob blob = builder.build();
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
               if (log.isTraceEnabled()) {
                  log.tracef("Futures, in order: %s", futures);
               }
               for (Future<?> f : futures) {
                  Object o = f.get();
                  if (log.isTraceEnabled()) {
                     log.tracef("Future %s returned %s", f, o);
                  }
               }
            } catch (InterruptedException ie) {
               Thread.currentThread().interrupt();
            } catch (ExecutionException ee) {
               exception = convertToCacheLoaderException("Caught exception in async process", ee
                     .getCause());
            }
            if (exception != null) {
               throw exception;
            }
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
            if (earliestExpiryTime == -1) {
               earliestExpiryTime = t;
            } else {
               earliestExpiryTime = Math.min(earliestExpiryTime, t);
            }
         }
      }

      try {
         final byte[] payloadBuffer = marshaller.objectToByteBuffer(bucket);
         if (cfg.isCompress()) {
            final byte[] compress = compress(payloadBuffer, blob);
            blob.setPayload(compress);
         } else {
            blob.setPayload(payloadBuffer);
        }
         if (earliestExpiryTime > -1) {
            Map<String, String> md = Collections.singletonMap(EARLIEST_EXPIRY_TIME, String
                  .valueOf(earliestExpiryTime));
            blob.getMetadata().setUserMetadata(md);
         }
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      } catch (InterruptedException ie) {
         if (log.isTraceEnabled()) {
            log.trace("Interrupted while writing blob");
        }
         Thread.currentThread().interrupt();
      }
   }

   private Bucket readFromBlob(Blob blob, String bucketName) throws CacheLoaderException {
      if (blob == null) {
        return null;
      }
      try {
         Bucket bucket;
         final InputStream content = blob.getPayload().getInput();
         if (cfg.isCompress()) {
            bucket = uncompress(blob, bucketName, content);
         } else {
            bucket = (Bucket) marshaller.objectFromInputStream(content);
         }

         if (bucket != null) {
            bucket.setBucketId(bucketName);
         }
         return bucket;
      } catch (ClassNotFoundException e) {
         throw convertToCacheLoaderException("Unable to read blob", e);
      } catch (IOException e) {
         throw convertToCacheLoaderException("Class loading issue", e);
      }
   }

   private Bucket uncompress(Blob blob, String bucketName, InputStream content) throws IOException, CacheLoaderException, ClassNotFoundException {
      //TODO go back to fully streamed version and get rid of the byte buffers
      BZip2CompressorInputStream is;
      Bucket bucket;
      ByteArrayOutputStream bos = new ByteArrayOutputStream();

      Streams.copy(content, bos);
      final byte[] compressedByteArray = bos.toByteArray();

      ByteArrayInputStream bis = new ByteArrayInputStream(compressedByteArray);

      is = new BZip2CompressorInputStream(bis);
      ByteArrayOutputStream bos2 = new ByteArrayOutputStream();
      Streams.copy(is, bos2);
      final byte[] uncompressedByteArray = bos2.toByteArray();

      byte[] md5FromStoredBlob = blob.getMetadata().getContentMetadata().getContentMD5();

      // not all blobstores support md5 on GET request
      if (md5FromStoredBlob != null){
         byte[] hash = getMd5Digest(compressedByteArray);
         if (!Arrays.equals(hash, md5FromStoredBlob)) {
            throw new CacheLoaderException("MD5 hash failed when reading (transfer error) for entry " + bucketName);
         }
      }

      is.close();
      bis.close();
      bos.close();
      bos2.close();

      bucket = (Bucket) marshaller
            .objectFromInputStream(new ByteArrayInputStream(uncompressedByteArray));
      return bucket;
   }

   private byte[] compress(final byte[] uncompressedByteArray, Blob blob) throws IOException {
      //TODO go back to fully streamed version and get rid of the byte buffers
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();

      InputStream input = new ByteArrayInputStream(uncompressedByteArray);
      BZip2CompressorOutputStream output = new BZip2CompressorOutputStream(baos);

      Streams.copy(input, output);

      output.close();
      input.close();

      final byte[] compressedByteArray = baos.toByteArray();

      blob.getMetadata().getContentMetadata().setContentMD5(getMd5Digest(compressedByteArray));

      baos.close();

      return compressedByteArray;
   }

   private String encodeBucketName(String bucketId) {
      final String name = bucketId.startsWith("-") ? bucketId.replace('-', 'A')
                                                   : bucketId;
      if (cfg.isCompress()) {
         return name + ".bz2";
      }
      return name;
   }

   private synchronized byte[] getMd5Digest(byte[] toDigest) {
      md5.reset();
      return md5.digest(toDigest);
   }
}
