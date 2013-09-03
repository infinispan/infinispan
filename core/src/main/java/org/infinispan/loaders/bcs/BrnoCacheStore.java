package org.infinispan.loaders.bcs;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.BrnoCacheStoreConfiguration;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.spi.AbstractCacheStore;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Local file-based cache store, optimized for write-through use with strong consistency guarantees
 * (ability to flush disk operations before returning from the store call).
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class BrnoCacheStore extends AbstractCacheStore {

   private static final Log log = LogFactory.getLog(BrnoCacheStore.class);

   private BrnoCacheStoreConfiguration configuration;
   private TemporaryTable temporaryTable;
   private IndexQueue indexQueue;
   private SyncProcessingQueue<LogRequest> storeQueue;
   private FileProvider fileProvider;
   private LogAppender logAppender;
   private Index index;
   private Compactor compactor;

   @Override
   public void init(CacheLoaderConfiguration cfg, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      configuration = validateConfigurationClass(cfg, BrnoCacheStoreConfiguration.class);
      super.init(cfg, cache, m);
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      log.info("Starting using configuration " + configuration);
      temporaryTable = new TemporaryTable();
      storeQueue = new SyncProcessingQueue<LogRequest>();
      indexQueue = new IndexQueue(configuration.indexSegments(), configuration.indexQueueLength());
      fileProvider = new FileProvider(configuration.dataLocation(), configuration.openFilesLimit());
      compactor = new Compactor(fileProvider, temporaryTable, indexQueue, marshaller, configuration.maxFileSize(), configuration.compactionThreshold());
      logAppender = new LogAppender(storeQueue, indexQueue, temporaryTable, fileProvider, configuration.syncWrites(), configuration.maxFileSize());
      try {
         index = new Index(fileProvider, configuration.indexLocation(), configuration.indexSegments(),
               configuration.minNodeSize(), configuration.maxNodeSize(),
               indexQueue, temporaryTable, compactor);
      } catch (IOException e) {
         throw new CacheLoaderException("Cannot open index file in " + configuration.indexLocation(), e);
      }
      compactor.setIndex(index);
      final AtomicLong maxSeqId = new AtomicLong(0);
      if (configuration.purgeOnStartup()) {
         log.debug("Not building the index - purge will be executed");
      } else {
         log.debug("Building the index");
         forEachOnDisk(false, new EntryFunctor() {
            @Override
            public boolean apply(int file, int offset, int size, byte[] serializedKey, byte[] serializedValue, long seqId, long expiration) throws IOException, ClassNotFoundException {
               long prevSeqId;
               if (seqId > (prevSeqId = maxSeqId.get())) {
                  while (!maxSeqId.compareAndSet(prevSeqId, seqId)) prevSeqId = maxSeqId.get();
               }
               Object key = marshaller.objectFromByteBuffer(serializedKey);
               try {
                  // We may check the seqId safely as we are the only thread writing to index
                  EntryPosition entry = temporaryTable.get(key);
                  if (entry == null) {
                     entry = index.getPosition(key, serializedKey);
                  }
                  if (entry != null) {
                     FileProvider.Handle handle = fileProvider.getFile(entry.file);
                     try {
                        EntryReaderWriter.EntryHeader header = EntryReaderWriter.readEntryHeader(handle, entry.offset);
                        if (header == null) {
                           throw new IllegalStateException("Cannot read " + entry.file + ":" + entry.offset);
                        }
                        if (seqId < header.seqId()) {
                           return true;
                        }
                     } finally {
                        handle.close();
                     }
                  }
                  temporaryTable.set(key, file, offset);
                  indexQueue.put(new IndexRequest(key, serializedKey, file, offset, size));
               } catch (InterruptedException e) {
                  log.error("Interrupted building of index, the index won't be built properly!", e);
                  return false;
               }
               return true;
            }
         });
      }
      logAppender.setSeqId(0);
   }

   @Override
   public void stop() throws CacheLoaderException {
      super.stop();
      try {
         logAppender.stopOperations();
         logAppender = null;
         compactor.stopOperations();
         compactor = null;
         index.stopOperations();
         index = null;
         fileProvider = null;
         temporaryTable = null;
         indexQueue = null;
         storeQueue = null;
      } catch (InterruptedException e) {
         throw new CacheLoaderException("Cannot stop cache store", e);
      }
      super.stop();
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      // expired entries are purged continually
   }

   @Override
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      throw new UnsupportedOperationException();
   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      throw new UnsupportedOperationException();
   }

   @Override
   public synchronized void clear() throws CacheLoaderException {
      try {
         logAppender.clearAndPause();
         compactor.clearAndPause();
      } catch (InterruptedException e) {
         throw new CacheLoaderException("Cannot pause cache store to clear it.", e);
      }
      try {
         index.clear();
      } catch (IOException e) {
         throw new CacheLoaderException("Cannot clear/reopen index!", e);
      }
      try {
         fileProvider.clear();
      } catch (IOException e) {
         throw new CacheLoaderException("Cannot clear data directory!", e);
      }
      temporaryTable.clear();
      compactor.resumeAfterPause();
      logAppender.resumeAfterPause();
   }

   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      try {
         storeQueue.pushAndWait(LogRequest.storeRequest(entry, marshaller));
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      try {
         LogRequest request = LogRequest.deleteRequest(key, marshaller);
         storeQueue.pushAndWait(request);
         return (Boolean) request.getIndexRequest().getResult();
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   @Override
   public boolean containsKey(Object key) throws CacheLoaderException {
      EntryPosition entry = temporaryTable.get(key);
      if (entry == null) {
         try {
            entry = index.getPosition(key, marshaller.objectToByteBuffer(key));
         } catch (Exception e) {
            throw new CacheLoaderException("Cannot load key from index", e);
         }
      }
      return entry != null && entry.offset >= 0;
   }

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      try {
         byte[] serializedValue;
         for (;;) {
            EntryPosition entry = temporaryTable.get(key);
            if (entry != null) {
               if (entry.offset < 0) {
                  return null;
               }
               FileProvider.Handle handle = fileProvider.getFile(entry.file);
               if (handle != null) {
                  try {
                     EntryReaderWriter.EntryHeader header = EntryReaderWriter.readEntryHeader(handle, entry.offset);
                     if (header == null) {
                        throw new IllegalStateException("Error reading from " + entry.file + ":" + entry.offset + " | " + handle.getFileSize());
                     }
                     if (header.expiryTime() > 0 && header.expiryTime() < System.currentTimeMillis()) {
                        return null;
                     }
                     if (header.valueLength() > 0) {
                        serializedValue = EntryReaderWriter.readValue(handle, header, entry.offset);
                        break;
                     } else {
                        return null;
                     }
                  } finally {
                     handle.close();
                  }
               }
            } else {
               serializedValue = index.getValue(key, marshaller.objectToByteBuffer(key));
               if (serializedValue == null) {
                  return null;
               }
               break;
            }
         }
         return ((InternalCacheValue) marshaller.objectFromByteBuffer(serializedValue)).toInternalCacheEntry(key);
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   private interface EntryFunctor {
      boolean apply(int file, int offset, int size, byte[] serializedKey, byte[] serializedValue, long seqId, long expiration) throws Exception;
   }

   private void forEachOnDisk(boolean readValues, EntryFunctor functor) throws CacheLoaderException {
      try {
         Iterator<Integer> iterator = fileProvider.getFileIterator();
         while (iterator.hasNext()) {
            int file = iterator.next();
            log.debug("Loading entries from file " + file);
            FileProvider.Handle handle = fileProvider.getFile(file);
            if (handle == null) {
               log.debug("File " + file + " was deleted during iteration");
               continue;
            }
            try {
               int offset = 0;
               for (;;) {
                  EntryReaderWriter.EntryHeader header = EntryReaderWriter.readEntryHeader(handle, offset);
                  if (header == null) {
                     break; // end of file;
                  }
                  try {
                     byte[] serializedKey = EntryReaderWriter.readKey(handle, header, offset);
                     if (serializedKey == null) {
                        break; // we have read the file concurrently with writing there
                        //throw new CacheLoaderException("File " + file + " appears corrupt when reading key from " + offset + ": header is " + header);
                     }
                     byte[] serializedValue = null;
                     int offsetOrNegation = offset;
                     if (header.valueLength() > 0) {
                        if (header.expiryTime() >= 0 && header.expiryTime() < System.currentTimeMillis()) {
                           offsetOrNegation = ~offset;
                        } else if (readValues) {
                           serializedValue = EntryReaderWriter.readValue(handle, header, offset);
                        }
                     } else {
                        offsetOrNegation = ~offset;
                     }
                     if (!functor.apply(file, offsetOrNegation, header.totalLength(), serializedKey, serializedValue, header.seqId(), header.expiryTime())) {
                        return;
                     }
                  } finally {
                     offset += header.totalLength();
                  }
               }
            } finally {
               handle.close();
            }
         }
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return load(-1);
   }

   private static class SequencedICE {
      public InternalCacheEntry ice;
      public long seqId;

      private SequencedICE(InternalCacheEntry ice, long seqId) {
         this.ice = ice;
         this.seqId = seqId;
      }
   }

   @Override
   public Set<InternalCacheEntry> load(final int numEntries) throws CacheLoaderException {
      if (numEntries == 0) return Collections.EMPTY_SET;
      // TODO: look to index instead of to memory-map
      final Map<Object, SequencedICE> map = new HashMap<Object, SequencedICE>();
      forEachOnDisk(true, new EntryFunctor() {
         @Override
         public boolean apply(int file, int offset, int size,
                              byte[] serializedKey, byte[] serializedValue, long seqId, long expiration) throws IOException, ClassNotFoundException {
            Object key = marshaller.objectFromByteBuffer(serializedKey);
            SequencedICE sequencedICE = map.get(key);
            InternalCacheEntry entry = null;
            if (serializedValue != null && (expiration < 0 || expiration > System.currentTimeMillis())) {
               entry = ((InternalCacheValue) marshaller.objectFromByteBuffer(serializedValue)).toInternalCacheEntry(key);
            }
            if (sequencedICE == null) {
               map.put(key, new SequencedICE(entry, seqId));
            } else if (sequencedICE.seqId < seqId) {
               sequencedICE.ice = entry;
               sequencedICE.seqId = seqId;
            }
            return numEntries < 0 || map.size() < numEntries;
         }
      });
      HashSet<InternalCacheEntry> set = new HashSet<InternalCacheEntry>();
      for (SequencedICE sequencedICE : map.values()) {
         if (sequencedICE.ice != null) {
            set.add(sequencedICE.ice);
         }
      }
      return set;
   }

   @Override
   public Set<Object> loadAllKeys(final Set<Object> keysToExclude) throws CacheLoaderException {
      final Set<Object> set = new HashSet<Object>();
      forEachOnDisk(false, new EntryFunctor() {
         @Override
         public boolean apply(int file, int offset, int size, byte[] serializedKey, byte[] serializedValue, long seqId, long expiration) throws Exception {
            Object key = marshaller.objectFromByteBuffer(serializedKey);
            if (keysToExclude == null || !keysToExclude.contains(key)) {
               set.add(key);
            }
            return true;
         }
      });
      return set;
   }
}
