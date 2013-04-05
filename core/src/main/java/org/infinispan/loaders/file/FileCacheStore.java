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

import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;
import org.infinispan.io.ExposedByteArrayOutputStream;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.loaders.bucket.BucketBasedCacheStore;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.CollectionFactory;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A filesystem-based implementation of a {@link org.infinispan.loaders.bucket.BucketBasedCacheStore}.  This file store
 * stores stuff in the following format: <tt>/{location}/cache name/bucket_number.bucket</tt>
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @since 4.0
 */
@CacheLoaderMetadata(configurationClass = FileCacheStoreConfig.class)
public class FileCacheStore extends BucketBasedCacheStore {

   static final Log log = LogFactory.getLog(FileCacheStore.class);
   private static final boolean trace = log.isTraceEnabled();
   private int streamBufferSize;

   FileCacheStoreConfig config;
   File root;
   FileSync fileSync;

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
         if (root != null && (listFiles = root.listFiles(NUMERIC_NAMED_FILES_FILTER)) != null) {
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
         File[] files = listFilesStrict(root, NUMERIC_NAMED_FILES_FILTER);

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
      File[] toDelete = root.listFiles(NUMERIC_NAMED_FILES_FILTER);
      if (toDelete == null) {
         return;
      }
      for (File f : toDelete) {
         deleteFile(f);
         if (f.exists()) {
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
      if (trace) log.trace("purgeInternal()");

      File[] files = listFilesStrict(root, NUMERIC_NAMED_FILES_FILTER);

      for (final File bucketFile : files) {
         if (multiThreadedPurge) {
            purgerService.execute(new Runnable() {
               @Override
               public void run() {
                  boolean interrupted = !doPurge(bucketFile);
                  if (interrupted) log.debug("Interrupted, so finish work.");
               }
            });
         } else {
            boolean interrupted = !doPurge(bucketFile);
            if (interrupted) {
               log.debug("Interrupted, so stop loading and finish with purging.");
               Thread.currentThread().interrupt();
            }
         }
      }
   }

   /**
    * Performs a purge on a given bucket file, and returns true if the process was <i>not</i> interrupted, false
    * otherwise.
    *
    * @param bucketFile bucket file to purge
    * @return true if the process was not interrupted, false otherwise.
    */
   private boolean doPurge(File bucketFile) {
      Integer bucketKey = Integer.valueOf(bucketFile.getName());
      boolean interrupted = false;
      try {
         lockForReading(bucketKey);
         Bucket bucket = loadBucket(bucketFile);

         if (bucket != null) {
            if (bucket.removeExpiredEntries()) {
               upgradeLock(bucketKey);
               updateBucket(bucket);
            }
         } else {
            // Bucket may be an empty 0-length file
            if (bucketFile.exists() && bucketFile.length() == 0) {
               upgradeLock(bucketKey);
               fileSync.deleteFile(bucketFile);
               if (bucketFile.exists())
                  log.info("Unable to remove empty file " + bucketFile + " - will try again later.");
            }
         }
      } catch (InterruptedException ie) {
         interrupted = true;
      } catch (CacheLoaderException e) {
         log.problemsPurgingFile(bucketFile, e);
      } finally {
         unlock(bucketKey);
      }
      return !interrupted;
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
         if (trace) {
            log.trace("Found bucket file: '" + bucketFile + "'");
         }
         InputStream is = null;
         try {
            // It could happen that the output buffer might not have been
            // flushed, so just in case, flush it to be able to read it.
            fileSync.flush(bucketFile);
            if (bucketFile.length() == 0) {
               // short circuit
               return null;
            }
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
         if (!purgeFile(f)) {
            log.problemsRemovingFile(f);
         } else if (trace) {
            log.tracef("Successfully deleted file: '%s'", f.getName());
         }
      }

      if (!b.getEntries().isEmpty()) {
         try {
            byte[] bytes = marshaller.objectToByteBuffer(b);
            fileSync.write(bytes, f);
         } catch (IOException ex) {
            log.errorSavingBucket(b, ex);
            throw new CacheLoaderException(ex);
         } catch (InterruptedException ie) {
            if (trace) {
               log.trace("Interrupted while marshalling a bucket");
            }
            Thread.currentThread().interrupt(); // Restore interrupted status
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

      FileCacheStoreConfig.FsyncMode fsyncMode = config.getFsyncMode();
      switch (fsyncMode) {
         case DEFAULT:
            fileSync = new BufferedFileSync();
            break;
         case PER_WRITE:
            fileSync = new PerWriteFileSync();
            break;
         case PERIODIC:
            fileSync = new PeriodicFileSync(config.getFsyncInterval());
            break;
      }

      log.debugf("Using %s file sync mode", fsyncMode);
   }

   @Override
   public void stop() throws CacheLoaderException {
      super.stop();
      fileSync.stop();
   }

   public Bucket loadBucketContainingKey(String key) throws CacheLoaderException {
      return loadBucket(getLockFromKey(key));
   }

   private void deleteFile(File f) {
      if (trace) {
         log.tracef("Really delete file %s", f);
      }
      fileSync.deleteFile(f);
   }

   private boolean purgeFile(File f) {
      if (trace) {
         log.tracef("Really clear file %s", f);
      }
      try {
         fileSync.purge(f);
         return true;
      } catch (IOException e) {
         if (trace)
            log.trace("Error encountered while clearing file: " + f, e);
         return false;
      }
   }

   private Object objectFromInputStreamInReentrantMode(InputStream is) throws IOException, ClassNotFoundException, InterruptedException {
      int len = is.available();
      Object o = null;
      if (len != 0) {
         ExposedByteArrayOutputStream bytes = new ExposedByteArrayOutputStream(len);
         byte[] buf = new byte[Math.min(len, 1024)];
         int bytesRead;
         while ((bytesRead = is.read(buf, 0, buf.length)) != -1) {
            bytes.write(buf, 0, bytesRead);
         }
         is = new ByteArrayInputStream(bytes.getRawBuffer(), 0, bytes.size());
         ObjectInput unmarshaller = marshaller.startObjectInput(is, false);
         try {
            o = marshaller.objectFromObjectStream(unmarshaller);
         } finally {
            marshaller.finishObjectInput(unmarshaller);
         }
      }
      return o;
   }

   /**
    * Returns an array of abstract pathnames denoting the files and
    * directories in the directory denoted by the given file abstract pathname
    * that satisfy the specified filter.
    *
    * @param f abstract pathname
    * @param filter file filter
    * @return a non-null list of pathnames which could be empty
    * @throws CacheLoaderException is thrown if the list returned by
    * {@link File#listFiles(java.io.FilenameFilter)} is null
    */
   private File[] listFilesStrict(File f, FilenameFilter filter) throws CacheLoaderException {
      File[] files = f.listFiles(filter);
      if (files == null) {
         boolean exists = f.exists();
         boolean isDirectory = f.isDirectory();
         boolean canRead = f.canRead();
         boolean canWrite = f.canWrite();
         throw new CacheLoaderException(String.format(
               "File %s is not directory or IO error occurred when listing " +
               "files with filter %s [fileExists=%b, isDirector=%b, " +
               "canRead=%b, canWrite=%b]",
               f, filter, exists, isDirectory, canRead, canWrite));
      }
      return files;
   }

   /**
    * Specifies how the changes written to a file will be synched with the underlying file system.
    */
   private interface FileSync {

      /**
       * Writes the given bytes to the file.
       *
       * @param bytes byte array containing the bytes to write.
       * @param f     File instance representing the location where to store the data.
       * @throws IOException if an I/O error occurs
       */
      void write(byte[] bytes, File f) throws IOException;

      /**
       * Force the file changes to be flushed to the underlying file system. Client code calling this flush method
       * should in advance check whether the file exists and so this method assumes that check was already done.
       *
       * @param f File instance representing the location changes should be flushed to.
       * @throws IOException if an I/O error occurs
       */
      void flush(File f) throws IOException;

      /**
       * Forces the file to be purged. Implementations are free to decide what the best option should be here. For
       * example, whether to delete the file, whether to empty it...etc.
       *
       * @param f File instance that should be purged.
       * @throws IOException if an I/O error occurs
       */
      void purge(File f) throws IOException;

      /**
       * Stop the file synching mechanism. This offers implementors the opportunity to do any cleanup when the cache
       * stops.
       */
      void stop();

      void deleteFile(File f);
   }

   private static class BufferedFileSync implements FileSync {
      protected final ConcurrentMap<String, FileChannel> streams = CollectionFactory.makeConcurrentMap();

      @Override
      public void write(byte[] bytes, File f) throws IOException {
         if (bytes.length == 0) {
            // Short circuit for deleting files
            deleteFile(f);
            return;
         }

         String path = f.getPath();
         FileChannel channel = streams.get(path);
         if (channel == null) {
            channel = createChannel(f);
            FileChannel existingChannel = streams.putIfAbsent(path, channel);
            if (existingChannel != null) {
               Util.close(channel);
               channel = existingChannel;
            }
         } else if (!f.exists()) {
            f.createNewFile();
            FileChannel oldChannel = channel;
            channel = createChannel(f);
            boolean replaced = streams.replace(path, oldChannel, channel);
            if (replaced) {
               Util.close(oldChannel);
            } else {
               Util.close(channel);
               channel = streams.get(path);
            }
         }
         channel.write(ByteBuffer.wrap(bytes));
      }

      @Override
      public void deleteFile(File f) {
         String path = f.getPath();
         // First remove the channel from the map
         FileChannel fc = streams.remove(path);
         // Then close the channel (handles null params)
         Util.close(fc);
         // Now that the channel is closed we can delete the file
         f.delete();
      }

      private FileChannel createChannel(File f) throws FileNotFoundException {
         return new RandomAccessFile(f, "rw").getChannel();
      }

      @Override
      public void flush(File f) throws IOException {
         FileChannel channel = streams.get(f.getPath());
         if (channel != null)
            channel.force(false);
      }

      @Override
      public void purge(File f) throws IOException {
         // Avoid a delete per-se because it hampers any fsync-like functionality
         // cos any cached file channel write won't change the file's exists
         // status. So, clear the file rather than delete it.
         FileChannel channel = streams.get(f.getPath());
         if (channel == null) {
            channel = createChannel(f);
            String path = f.getPath();
            FileChannel existingChannel = streams.putIfAbsent(path, channel);
            if (existingChannel != null) {
               Util.close(channel);
               channel = existingChannel;
            }
         }
         channel.truncate(0);
         // Apart from truncating, it's necessary to reset the position!
         channel.position(0);
      }

      @Override
      public void stop() {
         for (FileChannel channel : streams.values()) {
            try {
               channel.force(true);
            } catch (IOException e) {
               log.errorFlushingToFileChannel(channel, e);
            }
            Util.close(channel);
         }

         streams.clear();
      }

   }

   private class PeriodicFileSync extends BufferedFileSync {
      private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor();
      protected final ConcurrentMap<String, IOException> flushErrors = CollectionFactory.makeConcurrentMap();

      private PeriodicFileSync(long interval) {
         executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
               for (Map.Entry<String, FileChannel> entry : streams.entrySet()) {
                  if (trace)
                     log.tracef("Flushing channel in %s", entry.getKey());
                  FileChannel channel = entry.getValue();
                  try {
                     channel.force(true);
                  } catch (IOException e) {
                     if (trace)
                        log.tracef(e, "Error flushing output stream for %s", entry.getKey());
                     flushErrors.putIfAbsent(entry.getKey(), e);
                     // If an error is encountered, close it. Next time it's used,
                     // the exception will be propagated back to the user.
                     Util.close(channel);
                  }
               }
            }
         }, interval, interval, TimeUnit.MILLISECONDS);
      }

      @Override
      public void write(byte[] bytes, File f) throws IOException {
         String path = f.getPath();
         IOException error = flushErrors.get(path);
         if (error != null)
            throw new IOException(String.format(
                  "Periodic flush of channel for %s failed", path), error);

         super.write(bytes, f);
      }

      @Override
      public void stop() {
         executor.shutdown();
         super.stop();
      }
   }

   private static class PerWriteFileSync implements FileSync {
      @Override
      public void write(byte[] bytes, File f) throws IOException {
         FileOutputStream fos = null;
         try {
            if (bytes.length > 0) {
               fos = new FileOutputStream(f);
               fos.write(bytes);
               fos.flush();
               fos.getChannel().force(true);
            } else {
               deleteFile(f);
            }
         } finally {
            if (fos != null)
               fos.close();
         }
      }

      @Override
      public void flush(File f) throws IOException {
         // No-op since flush always happens upon write
      }

      @Override
      public void purge(File f) throws IOException {
         deleteFile(f);
      }

      @Override
      public void stop() {
         // No-op
      }

      @Override
      public void deleteFile(File f) {
         f.delete();
      }
   }

   /**
    * Makes sure that files opened are actually named as numbers (ignore all other files)
    */
   public static class NumericNamedFilesFilter implements FilenameFilter {
      @Override
      public boolean accept(File dir, String name) {
         int l = name.length();
         int s = name.charAt(0) == '-' ? 1 : 0;
         if (l - s > 10) {
            log.cacheLoaderIgnoringUnexpectedFile(dir, name);
            return false;
         }
         for (int i = s; i < l; i++) {
            char c = name.charAt(i);
            if (c < '0' || c > '9') {
               log.cacheLoaderIgnoringUnexpectedFile(dir, name);
               return false;
            }
         }
         return true;
      }
   }

   public static final NumericNamedFilesFilter NUMERIC_NAMED_FILES_FILTER = new NumericNamedFilesFilter();

}
