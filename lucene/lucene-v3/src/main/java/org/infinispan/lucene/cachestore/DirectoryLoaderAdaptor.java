package org.infinispan.lucene.cachestore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.store.IndexInput;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileListCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.infinispan.lucene.FileReadLockKey;
import org.infinispan.lucene.IndexScopedKey;
import org.infinispan.lucene.KeyVisitor;
import org.infinispan.lucene.logging.Log;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.LogFactory;

/**
 * Contains the low-level logic to map the cache structure the the "native"
 * Lucene format for a single Directory instance.
 * 
 * @author Sanne Grinovero
 * @since 5.2
 */
final class DirectoryLoaderAdaptor {

   private static final Log log = LogFactory.getLog(DirectoryLoaderAdaptor.class, Log.class);

   private final InternalDirectoryContract directory;
   private final LoadVisitor loadVisitor = new LoadVisitor();
   private final ContainsKeyVisitor containsKeyVisitor = new ContainsKeyVisitor();
   private final String indexName;
   private final int autoChunkSize;

   /**
    * Create a new DirectoryLoaderAdaptor.
    * 
    * @param directory The {@link org.apache.lucene.store.Directory} to which delegate actual IO operations
    * @param indexName the index name
    * @param autoChunkSize index segments might be large; we'll split them in chunks of this amount of bytes
    */
   protected DirectoryLoaderAdaptor(final InternalDirectoryContract directory, String indexName, int autoChunkSize) {
      this.directory = directory;
      this.indexName = indexName;
      this.autoChunkSize = autoChunkSize;
   }

   /**
    * Loads all "entries" from the CacheLoader; considering this is actually a Lucene index,
    * that's going to transform segments in entries in a specific order, simplest entries first.
    * 
    * @param entriesCollector loaded entries are collected in this set
    * @param maxEntries to limit amount of entries loaded
    * @throws CacheLoaderException 
    */
   protected void loadAllEntries(final HashSet<MarshalledEntry> entriesCollector, final int maxEntries, StreamingMarshaller marshaller) {
      int existingElements = entriesCollector.size();
      int toLoadElements = maxEntries - existingElements;
      if (toLoadElements <= 0) {
         return;
      }
      HashSet<IndexScopedKey> keysCollector = new HashSet<IndexScopedKey>();
      loadSomeKeys(keysCollector, Collections.EMPTY_SET, toLoadElements);
      for (IndexScopedKey key : keysCollector) {
         Object value = load(key);
         if (value != null) {
            MarshalledEntry cacheEntry = new MarshalledEntryImpl(key, value, null, marshaller);
            entriesCollector.add(cacheEntry);
         }
      }
   }

   /**
    * Load some keys in the collector, excluding some and to a maximum number of collected (non-excluded) keys.
    * @param keysCollector the set where to add loaded keys to
    * @param keysToExclude which keys should not be loaded. Warning: can be null! Means all keys are to be returned
    * @param maxElements
    * @throws CacheLoaderException
    */
   private void loadSomeKeys(final HashSet<IndexScopedKey> keysCollector, final Set<IndexScopedKey> keysToExclude, final int maxElements) throws CacheLoaderException {
      if (maxElements <= 0) {
         return;
      }
      int collectedKeys = 0;
      //First we collect the (single) FileListCacheKey
      FileListCacheKey rootKey = new FileListCacheKey(indexName);
      if (keysToExclude==null || ! keysToExclude.contains(rootKey)) { //unless it was excluded
         if (keysCollector.add(rootKey) ) { //unless it was already collected
            collectedKeys++;
         }
      }
      try {
         //Now we collect first all FileCacheKey (keys for file metadata)
         String[] listAll = directory.listAll();
         for (String fileName : listAll) {
            if (collectedKeys >= maxElements) return;
            FileCacheKey key = new FileCacheKey(indexName, fileName);
            if (keysToExclude == null || !keysToExclude.contains(key)) {
               if (keysCollector.add(key)) {
                  if (++collectedKeys >= maxElements) return;
               }
            }
         }
         //Next we load the ChunkCacheKey (keys for file contents)
         for (String fileName : listAll) {
            int numChunksInt = figureChunksNumber(fileName);
            for (int i = 0; i < numChunksInt; i++) {
               //Inner loop: we actually have several Chunks per file name
               ChunkCacheKey key = new ChunkCacheKey(indexName, fileName, i, autoChunkSize);
               if (keysToExclude == null || !keysToExclude.contains(key)) {
                  if (keysCollector.add(key)) {
                     if (++collectedKeys >= maxElements) return;
                  }
               }
            }
         }
      } catch (IOException e) {
         throw log.exceptionInCacheLoader(e);
      }
   }

   /**
    * Guess in how many chunks we should split this file. Should return the same value consistently
    * for the same file (segments are immutable) so that a full segment can be rebuilt from the upper
    * layers without anyone actually specifying the chunks numbers.
    */
   private int figureChunksNumber(String fileName) throws IOException {
      long fileLength = directory.fileLength(fileName);
      return figureChunksNumber(fileName, fileLength, autoChunkSize);
   }

   /**
    * Index segment files might be larger than 2GB; so it's possible to have an autoChunksize
    * which is too low to contain all bytes in a single array (overkill anyway).
    * In this case we ramp up and try splitting with larger chunkSize values.
    */
   private static int figureChunksNumber(final String fileName, final long fileLength, int chunkSize) {
      if (chunkSize < 0) {
         throw new IllegalStateException("Overflow in rescaling chunkSize. File way too large?");
      }
      final long numChunks = (fileLength / chunkSize);
      if (numChunks > Integer.MAX_VALUE) {
         log.rescalingChunksize(fileName, fileLength, chunkSize);
         chunkSize = 32 * chunkSize;
         return figureChunksNumber(fileName, fileLength, chunkSize);
      }
      else {
         return (int)numChunks;
      }
   }

   /**
    * @param keysCollector the Set where to add loaded keys to
    * @param keysToExclude Could be null!
    * @throws CacheLoaderException
    */
   protected void loadAllKeys(final HashSet<IndexScopedKey> keysCollector, final Set<IndexScopedKey> keysToExclude) throws CacheLoaderException {
      loadSomeKeys(keysCollector, keysToExclude, Integer.MAX_VALUE);
   }

   /**
    * Closes the underlying Directory. After it's closed, no other invocations are expected on this Adapter; we don't check explicitly for it
    * as the Directory instance takes care of it.
    */
   protected void close() {
      try {
         directory.close();
      } catch (IOException e) {
         //log but continue execution: we might want to try closing more instance
         log.errorOnFSDirectoryClose(e);
      }
   }

   /**
    * Load the value for a specific key
    */
   protected Object load(final IndexScopedKey key) throws CacheLoaderException {
      try {
         return key.accept(loadVisitor);
      } catch (Exception e) {
         throw log.exceptionInCacheLoader(e);
      }
   }

   /**
    * @param key
    * @return true if the indexKey matches a loadable entry
    */
   protected boolean containsKey(final IndexScopedKey key) throws CacheLoaderException {
      try {
         final Boolean returnValue = key.accept(containsKeyVisitor);
         return returnValue.booleanValue();
      } catch (Exception e) {
         throw log.exceptionInCacheLoader(e);
      }
   }

   /**
    * Load implementation for FileListCacheKey; must return a
    * ConcurrentHashSet containing the names of all files in this Directory.
    */
   private Object loadIntern(final FileListCacheKey key) throws IOException {
      final String[] listAll = directory.listAll();
      final ConcurrentHashSet<String> fileNames = new ConcurrentHashSet<String>();
      for (String filename : listAll) {
         fileNames.add(filename);
      }
      return fileNames;
   }

   /**
    * Load implementation for FileCacheKey: must return the metadata of the
    * requested file.
    */
   private FileMetadata loadIntern(final FileCacheKey key) throws IOException {
      final String fileName = key.getFileName();
      final long fileModified = directory.fileModified(fileName);
      final long fileLength = directory.fileLength(fileName);
      // We're forcing the buffer size of a to-be-read segment to the full file size:
      final int bufferSize = (int) Math.min(fileLength, (long)autoChunkSize);
      final FileMetadata meta = new FileMetadata(bufferSize);
      meta.setLastModified(fileModified);
      meta.setSize(fileLength);
      return meta;
   }

   /**
    * Loads the actual byte array from a segment, in the range of a specific chunkSize.
    * Not that while the chunkSize is specified in this case, it's likely derived
    * from the invocations of other loading methods.
    */
   private byte[] loadIntern(final ChunkCacheKey key) throws IOException {
      final String fileName = key.getFileName();
      final long chunkId = key.getChunkId(); //needs to be long to upcast following operations
      int bufferSize = key.getBufferSize();
      final long seekTo = chunkId * bufferSize;
      final byte[] buffer;
      final IndexInput input = directory.openInput(fileName);
      final long length = input.length();
      try {
         if (seekTo != 0) {
            input.seek(seekTo);
         }
         bufferSize = (int) Math.min(length - seekTo, (long)bufferSize);
         buffer = new byte[bufferSize];
         input.readBytes(buffer, 0, bufferSize);
      }
      finally {
         input.close();
      }
      return buffer;
   }

   /**
    * ContainsKey implementation for chunk elements
    */
   private Boolean containsKeyIntern(final ChunkCacheKey chunkCacheKey) throws IOException {
      try {
         final long length = directory.fileLength(chunkCacheKey.getFileName());
         final int bufferSize = chunkCacheKey.getBufferSize();
         final int chunkId = chunkCacheKey.getChunkId();
         return Boolean.valueOf( (chunkId * bufferSize) < (length + bufferSize) );
      }
      catch (FileNotFoundException nfne) {
         //Ok, we might check for file existence first.. but it's reasonable to be
         //optimistic.
         return Boolean.FALSE;
      }
   }

   /**
    * ContainsKey implementation for chunk elements
    */
   protected Boolean containsKeyIntern(final FileCacheKey fileCacheKey) throws IOException {
      return Boolean.valueOf(directory.fileExists(fileCacheKey.getFileName()));
   }

   /**
    * Routes invocations to type-safe load operations
    */
   private final class LoadVisitor implements KeyVisitor {

      @Override
      public Object visit(final FileListCacheKey fileListCacheKey) throws IOException {
         return DirectoryLoaderAdaptor.this.loadIntern(fileListCacheKey);
      }

      @Override
      public Object visit(final ChunkCacheKey chunkCacheKey) throws IOException {
         return DirectoryLoaderAdaptor.this.loadIntern(chunkCacheKey);
      }

      @Override
      public Object visit(final FileCacheKey fileCacheKey) throws IOException {
         return DirectoryLoaderAdaptor.this.loadIntern(fileCacheKey);
      }

      @Override
      public Object visit(final FileReadLockKey fileReadLockKey) {
         //ReadLocks should not leak to the actual storage
         return null;
      }
   }

   /**
    * Routes invocations to type-safe containsKey operations
    */
   private final class ContainsKeyVisitor implements KeyVisitor<Boolean> {

      @Override
      public Boolean visit(final FileListCacheKey fileListCacheKey) throws IOException {
         //We already know this Directory exists, as it's a pre-condition for the creation if this.
         //Also, all existing directories are able to list contained files.
         return Boolean.TRUE;
      }

      @Override
      public Boolean visit(final ChunkCacheKey chunkCacheKey) throws IOException {
         return DirectoryLoaderAdaptor.this.containsKeyIntern(chunkCacheKey);
      }

      @Override
      public Boolean visit(final FileCacheKey fileCacheKey) throws IOException {
         return DirectoryLoaderAdaptor.this.containsKeyIntern(fileCacheKey);
      }

      @Override
      public Boolean visit(final FileReadLockKey fileReadLockKey) {
         //ReadLocks should not leak to the actual storage
         return Boolean.FALSE;
      }
   }

}
