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
package org.infinispan.loaders.file;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;
import org.infinispan.io.ExposedByteArrayOutputStream;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.loaders.bucket.BucketBasedCacheStore;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A filesystem-based implementation of a {@link org.infinispan.loaders.bucket.BucketBasedCacheStore}.  This file store
 * stores stuff in the following format: <tt>/{location}/cache name/bucket_number.bucket</tt>
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @since 4.0
 */
@CacheLoaderMetadata(configurationClass = FileCacheStoreConfig.class)
public class FileCacheStore extends BucketBasedCacheStore {

   static final Log log = LogFactory.getLog(FileCacheStore.class);
   private static final boolean trace = log.isTraceEnabled();
   private int streamBufferSize;

   FileCacheStoreConfig config;
   File root;

   /**
    * @return root directory where all files for this {@link org.infinispan.loaders.CacheStore CacheStore} are written.
    */
   public File getRoot() {
      return root;
   }

   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      this.config = (FileCacheStoreConfig) config;
   }

   @Override
   protected void loopOverBuckets(BucketHandler handler) throws CacheLoaderException {
      try {
         File[] listFiles;
         if (root != null && (listFiles = root.listFiles()) != null) {
            for (File bucketFile : listFiles) {
               Bucket bucket = loadBucket(bucketFile);
               if (handler.handle(bucket)) {
                  break;
               }
            }
         }
      } catch (InterruptedException ie) {
         if (log.isDebugEnabled()) {
            log.debug("Interrupted, so stop looping over buckets.");
        }
         Thread.currentThread().interrupt();
      }
   }

   @Override
   protected void fromStreamLockSafe(ObjectInput objectInput) throws CacheLoaderException {
      try {
         int numFiles = objectInput.readInt();
         byte[] buffer = new byte[streamBufferSize];
         int bytesRead, totalBytesRead = 0;
         for (int i = 0; i < numFiles; i++) {
            String fName = (String) objectInput.readObject();
            int numBytes = objectInput.readInt();
            FileOutputStream fos = new FileOutputStream(root.getAbsolutePath() + File.separator + fName);
            BufferedOutputStream bos = new BufferedOutputStream(fos, streamBufferSize);

            try {
               while (numBytes > totalBytesRead) {
                  if (numBytes - totalBytesRead > streamBufferSize) {
                     bytesRead = objectInput.read(buffer, 0, streamBufferSize);
                  } else {
                     bytesRead = objectInput.read(buffer, 0, numBytes - totalBytesRead);
                  }

                  if (bytesRead == -1) {
                     break;
                  }
                  totalBytesRead += bytesRead;
                  bos.write(buffer, 0, bytesRead);
               }
               bos.flush();
               fos.flush();
               totalBytesRead = 0;
            } finally {
               safeClose(bos);
               safeClose(fos);
            }
         }
      } catch (IOException e) {
         throw new CacheLoaderException("I/O error", e);
      } catch (ClassNotFoundException e) {
         throw new CacheLoaderException("Unexpected exception", e);
      }
   }

   @Override
   protected void toStreamLockSafe(ObjectOutput objectOutput) throws CacheLoaderException {
      try {
         File[] files = root.listFiles();
         if (files == null)
            throw new CacheLoaderException("Root not directory or IO error occurred");

         objectOutput.writeInt(files.length);
         byte[] buffer = new byte[streamBufferSize];
         for (File file : files) {
            int bytesRead, totalBytesRead = 0;
            BufferedInputStream bis = null;
            FileInputStream fileInStream = null;
            try {
               if (trace) {
                log.tracef("Opening file in %s", file);
               }
               fileInStream = new FileInputStream(file);
               int sz = fileInStream.available();
               bis = new BufferedInputStream(fileInStream);
               objectOutput.writeObject(file.getName());
               objectOutput.writeInt(sz);

               while (sz > totalBytesRead) {
                  bytesRead = bis.read(buffer, 0, streamBufferSize);
                  if (bytesRead == -1) {
                     break;
                  }
                  totalBytesRead += bytesRead;
                  objectOutput.write(buffer, 0, bytesRead);
               }
            } finally {
               Util.close(bis);
               Util.close(fileInStream);
            }
         }
      } catch (IOException e) {
         throw new CacheLoaderException("I/O exception while generating stream", e);
      }
   }

   @Override
   protected void clearLockSafe() throws CacheLoaderException {
      File[] toDelete = root.listFiles();
      if (toDelete == null) {
         return;
      }
      for (File f : toDelete) {
         if (!deleteFile(f)) {
            log.problemsRemovingFile(f);
         }
      }
   }

   @Override
   protected boolean supportsMultiThreadedPurge() {
      return true;
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      if (trace) {
         log.trace("purgeInternal()");
      }
      if (acquireGlobalLock(false)) {
         try {
            File[] files = root.listFiles();
            if (files == null)
               throw new CacheLoaderException("Root not directory or IO error occurred");

            for (final File bucketFile : files) {
               if (multiThreadedPurge) {
                  purgerService.execute(new Runnable() {
                     @Override
                     public void run() {
                        Bucket bucket;
                        try {
                           if ((bucket = loadBucket(bucketFile)) != null && bucket.removeExpiredEntries()) {
                              updateBucket(bucket);
                           }
                        } catch (InterruptedException ie) {
                           if (log.isDebugEnabled()) {
                              log.debug("Interrupted, so finish work.");
                           }
                        } catch (CacheLoaderException e) {
                           log.problemsPurgingFile(bucketFile, e);
                        }
                     }
                  });
               } else {
                  Bucket bucket;
                  if ((bucket = loadBucket(bucketFile)) != null && bucket.removeExpiredEntries()) {
                    updateBucket(bucket);
                  }
               }
            }
         } catch (InterruptedException ie) {
            if (log.isDebugEnabled()) {
                log.debug("Interrupted, so stop loading and finish with purging.");
            }
            Thread.currentThread().interrupt();
         } finally {
            releaseGlobalLock(false);
            if (trace) {
                log.trace("Exit purgeInternal()");
            }
         }
      } else {
         log.unableToAcquireLockToPurgeStore();
      }
   }

   @Override
   protected Bucket loadBucket(Integer hash) throws CacheLoaderException {
      try {
         return loadBucket(new File(root, String.valueOf(hash)));
      } catch (InterruptedException ie) {
         if (log.isDebugEnabled()) {
            log.debug("Interrupted, so stop loading bucket and return null.");
         }
         Thread.currentThread().interrupt();
         return null;
      }
   }

   protected Bucket loadBucket(File bucketFile) throws CacheLoaderException, InterruptedException {
      Bucket bucket = null;
      if (bucketFile.exists()) {
         if (log.isTraceEnabled()) {
            log.trace("Found bucket file: '" + bucketFile + "'");
         }
         FileInputStream is = null;
         try {
            is = new FileInputStream(bucketFile);
            bucket = (Bucket) objectFromInputStreamInReentrantMode(is);
         } catch (InterruptedException ie) {
            throw ie;
         } catch (Exception e) {
            log.errorReadingFromFile(bucketFile.getAbsoluteFile(), e);
            throw new CacheLoaderException("Error while reading from file", e);
         } finally {
            safeClose(is);
         }
      }
      if (bucket != null) {
         bucket.setBucketId(bucketFile.getName());
      }
      return bucket;
   }

   @Override
   public void updateBucket(Bucket b) throws CacheLoaderException {
      File f = new File(root, b.getBucketIdAsString());
      if (f.exists()) {
         if (!deleteFile(f)) {
            log.problemsRemovingFile(f);
         }
      } else if (log.isTraceEnabled()) {
         log.tracef("Successfully deleted file: '%s'", f.getName());
      }

      if (!b.getEntries().isEmpty()) {
         FileOutputStream fos = null;
         try {
            byte[] bytes = marshaller.objectToByteBuffer(b);
            fos = new FileOutputStream(f);
            fos.write(bytes);
            fos.flush();
         } catch (IOException ex) {
            log.errorSavingBucket(b, ex);
            throw new CacheLoaderException(ex);
         } catch (InterruptedException ie) {
            if (trace) {
               log.trace("Interrupted while marshalling a bucket");
            }
            Thread.currentThread().interrupt(); // Restore interrupted status
         }
         finally {
            safeClose(fos);
         }
      }
   }

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return FileCacheStoreConfig.class;
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      String location = config.getLocation();
      if (location == null || location.trim().length() == 0) {
         location = "Infinispan-FileCacheStore"; // use relative path!
      }
      location += File.separator + cache.getName();
      root = new File(location);
      if (!root.exists()) {
         if (!root.mkdirs()) {
            log.problemsCreatingDirectory(root);
         }
      }
      if (!root.exists()) {
         throw new ConfigurationException("Directory " + root.getAbsolutePath() + " does not exist and cannot be created!");
      }
      streamBufferSize = config.getStreamBufferSize();
   }

   public Bucket loadBucketContainingKey(String key) throws CacheLoaderException {
      return loadBucket(key.hashCode());
   }

   private boolean deleteFile(File f) {
      if (trace) {
         log.tracef("Really delete file %s", f);
      }
      return f.delete();
   }

   private Object objectFromInputStreamInReentrantMode(InputStream is) throws IOException, ClassNotFoundException, InterruptedException {
      int len = is.available();
      ExposedByteArrayOutputStream bytes = new ExposedByteArrayOutputStream(len);
      byte[] buf = new byte[Math.min(len, 1024)];
      int bytesRead;
      while ((bytesRead = is.read(buf, 0, buf.length)) != -1) {
         bytes.write(buf, 0, bytesRead);
      }
      is = new ByteArrayInputStream(bytes.getRawBuffer(), 0, bytes.size());
      ObjectInput unmarshaller = marshaller.startObjectInput(is, true);
      Object o = null;
      try {
         o = marshaller.objectFromObjectStream(unmarshaller);
      } finally {
         marshaller.finishObjectInput(unmarshaller);
      }
      return o;
   }
}
