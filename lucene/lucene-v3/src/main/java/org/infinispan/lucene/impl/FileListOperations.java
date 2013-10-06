package org.infinispan.lucene.impl;

import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileListCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Collects operations on the existing fileList, stored as a Set<String> having key
 * of type FileListCacheKey(indexName).
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
@SuppressWarnings("unchecked")
public final class FileListOperations {

   private static final Log log = LogFactory.getLog(InfinispanIndexOutput.class);
   private static final boolean trace = log.isTraceEnabled();

   private final FileListCacheKey fileListCacheKey;
   private final AdvancedCache<FileListCacheKey, Object> cache;
   private final String indexName;
   private final AdvancedCache<FileListCacheKey, Set<String>> cacheNoRetrieve;

   public FileListOperations(AdvancedCache<?, ?> cache, String indexName){
      this.cache = (AdvancedCache<FileListCacheKey, Object>) cache;
      this.cacheNoRetrieve = (AdvancedCache<FileListCacheKey, Set<String>>) cache.withFlags(Flag.IGNORE_RETURN_VALUES);
      this.indexName = indexName;
      this.fileListCacheKey = new FileListCacheKey(indexName);
   }

   /**
    * @return the current list of files being part of the index 
    */
   public Set<String> getFileList() {
      Set<String> fileList = (Set<String>) cache.get(fileListCacheKey);
      if (fileList == null) {
         fileList = new ConcurrentHashSet<String>();
         Set<String> prev = (Set<String>) cache.putIfAbsent(fileListCacheKey, fileList);
         if ( prev != null ) {
            fileList = prev;
         }
      }
      if (trace)
         log.trace("Refreshed file listing view");
      return fileList;
   }

   /**
    * Deleted a file from the list of files actively part of the index
    * @param fileName
    */
   public void deleteFileName(String fileName) {
      Set<String> fileList = getFileList();
      boolean done = fileList.remove(fileName);
      if (done) {
         cacheNoRetrieve.put(fileListCacheKey, fileList);
         if (trace)
            log.trace("Updated file listing: removed " + fileName);
      }
   }
   
   /**
    * Adds a new fileName in the list of files making up this index
    * @param fileName
    */
   void addFileName(String fileName) {
      Set<String> fileList = getFileList();
      boolean done = fileList.add(fileName);
      if (done) {
         cacheNoRetrieve.put(fileListCacheKey, fileList);
         if (trace)
            log.trace("Updated file listing: added " + fileName);
      }
   }
   
   /**
    * @param fileName
    * @return the FileMetadata associated with the fileName, or null if the file wasn't found.
    */
   public FileMetadata getFileMetadata(String fileName) {
      FileCacheKey key = new FileCacheKey(indexName, fileName);
      FileMetadata metadata = (FileMetadata) cache.get(key);
      return metadata;
   }

   /**
    * Optimized implementation to perform both a remove and an add 
    * @param toRemove
    * @param toAdd
    */
   public void removeAndAdd(String toRemove, String toAdd) {
      Set<String> fileList = getFileList();
      boolean doneAdd = fileList.add(toAdd);
      boolean doneRemove = fileList.remove(toRemove);
      if (doneAdd || doneRemove) {
         cacheNoRetrieve.put(fileListCacheKey, fileList);
         if (trace) {
            if (doneAdd) log.trace("Updated file listing: added " + toAdd);
            if (doneRemove) log.trace("Updated file listing: removed " + toRemove);
         }
      }
   }

}
