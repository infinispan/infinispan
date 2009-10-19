package org.infinispan.loaders.file;

import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.loaders.bucket.BucketBasedCacheStore;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;

/**
 * A filesystem-based implementation of a {@link org.infinispan.loaders.bucket.BucketBasedCacheStore}.  This file store
 * stores stuff in the following format: <tt>/{location}/cache name/bucket_number.bucket</tt>
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class FileCacheStore extends BucketBasedCacheStore {

   private static final Log log = LogFactory.getLog(FileCacheStore.class);
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
   public void init(CacheLoaderConfig config, Cache cache, Marshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      this.config = (FileCacheStoreConfig) config;
   }

   protected Set<InternalCacheEntry> loadAllLockSafe() throws CacheLoaderException {
      Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>();
      for (File bucketFile : root.listFiles()) {
         Bucket bucket = loadBucket(bucketFile);
         if (bucket != null) {
            if (bucket.removeExpiredEntries()) {
               saveBucket(bucket);
            }
            result.addAll(bucket.getStoredEntries());
         }
      }
      return result;
   }

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

            while (numBytes > totalBytesRead) {
               if ((numBytes - totalBytesRead) > streamBufferSize) {
                  bytesRead = objectInput.read(buffer, 0, streamBufferSize);
               } else {
                  bytesRead = objectInput.read(buffer, 0, numBytes - totalBytesRead);
               }
               
               if (bytesRead == -1) break;
               totalBytesRead += bytesRead;
               bos.write(buffer, 0, bytesRead);
            }
            bos.flush();
            safeClose(bos);
            fos.flush();
            safeClose(fos);
            totalBytesRead = 0;
         }
      } catch (IOException e) {
         throw new CacheLoaderException("I/O error", e);
      } catch (ClassNotFoundException e) {
         throw new CacheLoaderException("Unexpected expcetion", e);
      }
   }

   protected void toStreamLockSafe(ObjectOutput objectOutput) throws CacheLoaderException {
      try {
         File[] files = root.listFiles();
         objectOutput.writeInt(files.length);
         byte[] buffer = new byte[streamBufferSize];
         for (File file : files) {
            int bytesRead, totalBytesRead = 0;
            BufferedInputStream bis = null;
            FileInputStream fileInStream = null;
            try {
               fileInStream = new FileInputStream(file);
               int sz = fileInStream.available();
               bis = new BufferedInputStream(fileInStream);
               objectOutput.writeObject(file.getName());
               objectOutput.writeInt(sz);

               while (sz > totalBytesRead) {
                  bytesRead = bis.read(buffer, 0, streamBufferSize);
                  if (bytesRead == -1) break;
                  totalBytesRead += bytesRead;
                  objectOutput.write(buffer, 0, bytesRead);
               }
            } finally {
               Util.closeStream(bis);
               Util.closeStream(fileInStream);
            }
         }
      } catch (IOException e) {
         throw new CacheLoaderException("I/O expcetion while generating stream", e);
      }
   }

   protected void clearLockSafe() throws CacheLoaderException {
      File[] toDelete = root.listFiles();
      if (toDelete == null) {
         return;
      }
      for (File f : toDelete) {
         if (!f.delete()) {
            log.warn("Had problems removing file {0}", f);
         }
      }
   }

   protected void purgeInternal() throws CacheLoaderException {
      loadAll();
   }

   protected Bucket loadBucket(String bucketName) throws CacheLoaderException {
      return loadBucket(new File(root, bucketName));
   }

   protected Bucket loadBucket(File bucketFile) throws CacheLoaderException {
      Bucket bucket = null;
      if (bucketFile.exists()) {
         if (log.isTraceEnabled()) log.trace("Found bucket file: '" + bucketFile + "'");
         FileInputStream is = null;
         try {
            is = new FileInputStream(bucketFile);
            bucket = (Bucket) marshaller.objectFromInputStream(is);
         } catch (Exception e) {
            String message = "Error while reading from file: " + bucketFile.getAbsoluteFile();
            log.error(message, e);
            throw new CacheLoaderException(message, e);
         } finally {
            safeClose(is);
         }
      }
      if (bucket != null) {
         bucket.setBucketName(bucketFile.getName());
      }
      return bucket;
   }

   protected void insertBucket(Bucket bucket) throws CacheLoaderException {
      saveBucket(bucket);
   }

   public void saveBucket(Bucket b) throws CacheLoaderException {
      File f = new File(root, b.getBucketName());
      if (f.exists()) {
         if (!f.delete()) log.warn("Had problems removing file {0}", f);
      } else if (log.isTraceEnabled()) {
         log.trace("Successfully deleted file: '" + f.getName() + "'");
      }

      if (!b.getEntries().isEmpty()) {
         FileOutputStream fos = null;
         try {
            byte[] bytes = marshaller.objectToByteBuffer(b);
            fos = new FileOutputStream(f);
            fos.write(bytes);
            fos.flush();
         } catch (IOException ex) {
            log.error("Exception while saving bucket " + b, ex);
            throw new CacheLoaderException(ex);
         }
         finally {
            safeClose(fos);
         }
      }
   }

   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return FileCacheStoreConfig.class;
   }

   public void start() throws CacheLoaderException {
      super.start();
      String location = config.getLocation();
      if (location == null || location.trim().length() == 0)
         location = "Infinispan-FileCacheStore"; // use relative path!
      location += File.separator + cache.getName();
      root = new File(location);
      if(!root.exists()) {
         if (!root.mkdirs()) {
            log.warn("Problems creating the directory: " + root);  
         }
      }
      if (!root.exists()) {
         throw new ConfigurationException("Directory " + root.getAbsolutePath() + " does not exist and cannot be created!");
      }
      streamBufferSize = config.getStreamBufferSize();
   }

   public Bucket loadBucketContainingKey(String key) throws CacheLoaderException {
      return loadBucket(key.hashCode() + "");
   }
}
