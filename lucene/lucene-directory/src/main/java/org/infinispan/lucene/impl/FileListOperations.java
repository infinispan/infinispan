package org.infinispan.lucene.impl;

import net.jcip.annotations.GuardedBy;
import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileListCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Collects operations on the existing fileList, stored as a Set<String> having key
 * of type FileListCacheKey(indexName).
 *
 * @author Sanne Grinovero
 * @since 4.1
 */
public final class FileListOperations {

   private static final Log log = LogFactory.getLog(InfinispanIndexOutput.class);
   private final boolean trace = log.isTraceEnabled();

   private final FileListCacheKey fileListCacheKey;
   private final AdvancedCache<FileListCacheKey, Object> cache;
   private final String indexName;
   private final AdvancedCache<FileListCacheKey, FileListCacheValue> cacheNoRetrieve;
   private final Lock readLock;
   private final Lock writeLock;
   private final boolean writeAsync;

   @SuppressWarnings("unchecked")
   public FileListOperations(AdvancedCache<?, ?> cache, String indexName, boolean writeAsync) {
      this.writeAsync = writeAsync;
      this.cache = (AdvancedCache<FileListCacheKey, Object>) cache;
      this.cacheNoRetrieve = (AdvancedCache<FileListCacheKey, FileListCacheValue>) cache.withFlags(Flag.IGNORE_RETURN_VALUES);
      this.indexName = indexName;
      this.fileListCacheKey = new FileListCacheKey(indexName);
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
      readLock = lock.readLock();
      writeLock = lock.writeLock();
   }

   /**
    * Adds a new fileName in the list of files making up this index
    * @param fileName
    */
   void addFileName(final String fileName) {
      writeLock.lock();
      try {
         final FileListCacheValue fileList = getFileList();
         boolean done = fileList.add(fileName);
         if (done) {
            updateFileList(fileList);
            if (trace)
               log.trace("Updated file listing: added " + fileName);
         }
      } finally {
         writeLock.unlock();
      }
   }

   /**
    * @param fileName
    * @return the FileMetadata associated with the fileName, or null if the file wasn't found.
    */
   public FileMetadata getFileMetadata(final String fileName) {
      FileCacheKey key = new FileCacheKey(indexName, fileName);
      FileMetadata metadata = (FileMetadata) cache.get(key);
      return metadata;
   }

   /**
    * Optimized implementation to perform both a remove and an add
    * @param toRemove
    * @param toAdd
    */
   public void removeAndAdd(final String toRemove, final String toAdd) {
      writeLock.lock();
      try {
         FileListCacheValue fileList = getFileList();
         boolean done = fileList.addAndRemove(toAdd, toRemove);
         if (done) {
            updateFileList(fileList);
            if (trace) {
               log.trace("Updated file listing: added " + toAdd + " and removed " + toRemove);
            }
         }
      } finally {
         writeLock.unlock();
      }
   }

   /**
    * @return an array containing all names of existing "files"
    */
   public String[] listFilenames() {
      readLock.lock();
      try {
         return getFileList().toArray();
      } finally {
         readLock.unlock();
      }
   }

   /**
    * @param fileName
    * @return true if there is such a named file in this index
    */
   public boolean fileExists(final String fileName) {
      readLock.lock();
      try {
         return getFileList().contains(fileName);
      } finally {
         readLock.unlock();
      }
   }

   /**
    * Deleted a file from the list of files actively part of the index
    * @param fileName
    */
   public void deleteFileName(final String fileName) {
      writeLock.lock();
      try {
         FileListCacheValue fileList = getFileList();
         boolean done = fileList.remove(fileName);
         if (done) {
            updateFileList(fileList);
            if (trace)
               log.trace("Updated file listing: removed " + fileName);
         }
      } finally {
         writeLock.unlock();
      }
   }

   /**
    * Makes sure the Cache is updated.
    * @param fileList the new content
    */
   @GuardedBy("writeLock")
   private void updateFileList(FileListCacheValue fileList) {
      if (writeAsync) {
         cacheNoRetrieve.putAsync(fileListCacheKey, fileList);
      }
      else {
         cacheNoRetrieve.put(fileListCacheKey, fileList);
      }
   }

   /**
    * @return the current list of files being part of the index
    */
   @GuardedBy("writeLock")
   private FileListCacheValue getFileList() {
      FileListCacheValue fileList = (FileListCacheValue) cache.get(fileListCacheKey);
      if (fileList == null) {
         fileList = new FileListCacheValue();
         FileListCacheValue prev = (FileListCacheValue) cache.putIfAbsent(fileListCacheKey, fileList);
         if (prev != null) {
            fileList = prev;
         }
      }
      if (trace)
         log.trace("Refreshed file listing view");
      return fileList;
   }

}
