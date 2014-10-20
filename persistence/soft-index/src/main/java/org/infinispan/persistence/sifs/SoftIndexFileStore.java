package org.infinispan.persistence.sifs;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Local file-based cache store, optimized for write-through use with strong consistency guarantees
 * (ability to flush disk operations before returning from the store call).
 *
 * * DESIGN:
 * There are three threads operating in the cache-store:
 * - LogAppender:  Requests to store entries are passed to the LogAppender thread
 *                 via queue, then the requestor threads wait until LogAppender notifies
 *                 them about successful store. LogAppender serializes the writes
 *                 into append-only file, writes the offset into TemporaryTable
 *                 and enqueues request to update index into UpdateQueue.
 *                 The append-only files have limited size, when the file is full,
 *                 new file is started.
 * - IndexUpdater: Reads the UpdateQueue, applies the operation into B-tree-like
 *                 structure Index (exact description below) and then removes
 *                 the entry from TemporaryTable. When the Index is overwriten,
 *                 the current entry offset is retrieved and IndexUpdater increases
 *                 the unused space statistics in FileStats.
 * - Compactor:    When a limit of unused space in some file is reached (according
 *                 to FileStats), the Compactor starts reading this file sequentially,
 *                 querying TemporaryTable or Index for the current entry position
 *                 and copying the unchanged entries into another file. For the entries
 *                 that are still valid in the original file, a compare-and-set
 *                 (file-offset based) request is enqueued into UpdateQueue - therefore
 *                 this operation cannot interfere with concurrent writes overwriting
 *                 the entry. Multiple files can be merged into single file during
 *                 compaction.
 *
 * Structures:
 * - TemporaryTable: keeps the records about current entry location until this is
 *                   applied to the Index. Each read request goes to the TemporaryTable,
 *                   if the key is not found here, Index is queried.
 * - UpdateQueue:    bounded queue (to prevent grow the TemporaryTable too much) of either
 *                   forced writes (used for regular stores) or compare-and-set writes
 *                   (used by Compactor).
 * - FileStats:      simple (Concurrent)HashTable with actual file size and amount of unused
 *                   space for each file.
 * - Index:          B+-tree of IndexNodes. The tree is dropped and built a new if the process
 *                   crashes, it does not need to flush disk operations. On disk it is kept as single random-accessed file, with free blocks list stored in memory.
 *
 * As IndexUpdater may easily become a bottleneck under heavy load, the IndexUpdater thread,
 * UpdateQueue and tree of IndexNodes may be multiplied several times - the Index is divided
 * into Segments. Each segment owns keys according to the hashCode() of the key.
 *
 * Amount of entries in IndexNode is limited by the size it occupies on disk. This size is
 * limited by configurable nodeSize (4096 bytes by default?), only in case that the node
 * contains single pivot (too long) it can be longer. A key_prefix common for all keys
 * in the IndexNode is stored in order to reduce space requirements. For implementation
 * reasons the keys are limited to 32kB - this requirement may be circumvented later.
 *
 * The pivots are not whole keys - it is the shortest part of key that is greater than all
 * left children (but lesser or equal to all right children) - let us call this key_part.
 * The key_parts are sorted in the IndexNode, naturally. On disk it has this format:
 *
 *  key_prefix_length(2 bytes), key_prefix, num_parts(2 bytes),
 *     ( key_part_length (2 bytes), key_part, left_child_index_node_offset (8 bytes))+,
 *     right_child_index_node_offset (8 bytes)
 *
 * In memory, for every child a SoftReference<IndexNode> is held. When this reference
 * is empty (but the offset in file is set), any reader may load the reference using
 * double-locking pattern (synchronized over the reference itself). The entry is never
 * loaded by multiple threads in parallel and even may block other threads trying to
 * read this node.
 *
 * For each node in memory a RW-lock is held. When the IndexUpdater thread updates
 * the Index (modifying some IndexNodes), it prepares a copy of these nodes (already
 * stored into index file). Then, in locks only the uppermost node for writing, overwrites
 * the references to new data and unlocks the this node. After that the changed nodes are
 * traversed from top down, write locked and their record in index file is released.
 * Reader threads crawl the tree from top down, locking the parent node (for reading),
 * locking child node and unlocking parent node.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class SoftIndexFileStore implements AdvancedLoadWriteStore {

   private static final Log log = LogFactory.getLog(SoftIndexFileStore.class);
   private static final boolean trace = log.isTraceEnabled();

   private SoftIndexFileStoreConfiguration configuration;
   private TemporaryTable temporaryTable;
   private IndexQueue indexQueue;
   private SyncProcessingQueue<LogRequest> storeQueue;
   private FileProvider fileProvider;
   private LogAppender logAppender;
   private Index index;
   private Compactor compactor;
   private StreamingMarshaller marshaller;
   private ByteBufferFactory byteBufferFactory;
   private MarshalledEntryFactory marshalledEntryFactory;
   private TimeService timeService;
   private Equivalence<Object> keyEquivalence;
   private int maxKeyLength;

   @Override
   public void init(InitializationContext ctx) {
      configuration = ctx.getConfiguration();
      marshaller = ctx.getMarshaller();
      marshalledEntryFactory = ctx.getMarshalledEntryFactory();
      byteBufferFactory = ctx.getByteBufferFactory();
      timeService = ctx.getTimeService();
      keyEquivalence = ctx.getCache().getAdvancedCache().getCacheConfiguration().dataContainer().keyEquivalence();
      maxKeyLength = configuration.maxNodeSize() - IndexNode.RESERVED_SPACE;
   }

   @Override
   public void start() {
      log.info("Starting using configuration " + configuration);
      temporaryTable = new TemporaryTable(configuration.indexQueueLength() * configuration.indexSegments(), keyEquivalence);
      storeQueue = new SyncProcessingQueue<LogRequest>();
      indexQueue = new IndexQueue(configuration.indexSegments(), configuration.indexQueueLength(), keyEquivalence);
      fileProvider = new FileProvider(configuration.dataLocation(), configuration.openFilesLimit());
      compactor = new Compactor(fileProvider, temporaryTable, indexQueue, marshaller, timeService, configuration.maxFileSize(), configuration.compactionThreshold());
      logAppender = new LogAppender(storeQueue, indexQueue, temporaryTable, compactor, fileProvider, configuration.syncWrites(), configuration.maxFileSize());
      try {
         index = new Index(fileProvider, configuration.indexLocation(), configuration.indexSegments(),
               configuration.minNodeSize(), configuration.maxNodeSize(),
               indexQueue, temporaryTable, compactor, timeService, keyEquivalence);
      } catch (IOException e) {
         throw new PersistenceException("Cannot open index file in " + configuration.indexLocation(), e);
      }
      compactor.setIndex(index);
      final AtomicLong maxSeqId = new AtomicLong(0);
      if (configuration.purgeOnStartup()) {
         log.debug("Not building the index - purge will be executed");
      } else {
         log.debug("Building the index");
         forEachOnDisk(false, false, new EntryFunctor() {
            @Override
            public boolean apply(int file, int offset, int size, byte[] serializedKey, byte[] serializedMetadata, byte[] serializedValue, long seqId, long expiration) throws IOException, ClassNotFoundException {
               long prevSeqId;
               while (seqId > (prevSeqId = maxSeqId.get()) && !maxSeqId.compareAndSet(prevSeqId, seqId));
               Object key = marshaller.objectFromByteBuffer(serializedKey);
               if (trace) log.tracef("Loaded %d:%d (seqId %d, expiration %d)", file, offset, seqId, expiration);
               try {
                  // We may check the seqId safely as we are the only thread writing to index
                  EntryPosition entry = temporaryTable.get(key);
                  if (entry == null) {
                     entry = index.getPosition(key, serializedKey);
                  }
                  if (entry != null) {
                     FileProvider.Handle handle = fileProvider.getFile(entry.file);
                     try {
                        EntryHeader header = EntryRecord.readEntryHeader(handle, entry.offset);
                        if (header == null) {
                           throw new IllegalStateException("Cannot read " + entry.file + ":" + entry.offset);
                        }
                        if (seqId < header.seqId()) {
                           if (trace) log.tracef("Record on %d:%d has seqId %d > %d", entry.file, entry.offset, header.seqId(), seqId);
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
         }, new FileFunctor() {
            @Override
            public void afterFile(int file) {
               compactor.completeFile(file);
            }
         });
      }
      logAppender.setSeqId(0);
   }

   @Override
   public void stop() {
      try {
         logAppender.stopOperations();
         logAppender = null;
         compactor.stopOperations();
         compactor = null;
         index.stopOperations();
         index = null;
         fileProvider.stop();
         fileProvider = null;
         temporaryTable = null;
         indexQueue = null;
         storeQueue = null;
      } catch (InterruptedException e) {
         throw new PersistenceException("Cannot stop cache store", e);
      }
   }

   @Override
   public synchronized void clear() throws PersistenceException {
      try {
         logAppender.clearAndPause();
         compactor.clearAndPause();
      } catch (InterruptedException e) {
         throw new PersistenceException("Cannot pause cache store to clear it.", e);
      }
      try {
         index.clear();
      } catch (IOException e) {
         throw new PersistenceException("Cannot clear/reopen index!", e);
      }
      try {
         fileProvider.clear();
      } catch (IOException e) {
         throw new PersistenceException("Cannot clear data directory!", e);
      }
      temporaryTable.clear();
      compactor.resumeAfterPause();
      logAppender.resumeAfterPause();
   }

   @Override
   public synchronized int size() {
      try {
         logAppender.pause();
         return (int) index.size();
      } catch (InterruptedException e) {
         log.error("Interrupted", e);
         Thread.currentThread().interrupt();
         return -1;
      } finally {
         logAppender.resumeAfterPause();
      }
   }

   @Override
   public void purge(Executor threadPool, PurgeListener listener) {
      log.trace("Purge method not supported, ignoring.");
      // TODO: in future we may support to force compactor run on all files
   }

   @Override
   public void write(MarshalledEntry entry) {
      int keyLength = entry.getKeyBytes().getLength();
      if (keyLength > maxKeyLength) {
         throw new PersistenceException("Configuration 'maxNodeSize' is too low - with maxNodeSize="
               + configuration.maxNodeSize() + " bytes you can use only keys serialized to " + maxKeyLength
               + " bytes (key " + entry.getKey() + " is serialized to " + keyLength + " bytes)");
      } else if (keyLength > Short.MAX_VALUE) {
         // TODO this limitation could be removed by different key length encoding
         throw new PersistenceException("SoftIndexFileStore is limited to keys with serialized size <= 32767 bytes");
      }
      try {
         storeQueue.pushAndWait(LogRequest.storeRequest(entry));
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   @Override
   public boolean delete(Object key) {
      try {
         LogRequest request = LogRequest.deleteRequest(key, toBuffer(marshaller.objectToByteBuffer(key)));
         storeQueue.pushAndWait(request);
         return (Boolean) request.getIndexRequest().getResult();
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   @Override
   public boolean contains(Object key) {
      try {
         for (;;) {
            // TODO: consider storing expiration timestamp in temporary table
            EntryPosition entry = temporaryTable.get(key);
            if (entry != null) {
               if (entry.offset < 0) {
                  return false;
               }
               FileProvider.Handle handle = fileProvider.getFile(entry.file);
               if (handle != null) {
                  try {
                     EntryHeader header = EntryRecord.readEntryHeader(handle, entry.offset);
                     if (header == null) {
                        throw new IllegalStateException("Error reading from " + entry.file + ":" + entry.offset + " | " + handle.getFileSize());
                     }
                     return header.expiryTime() < 0 || header.expiryTime() > timeService.wallClockTime();
                  } finally {
                     handle.close();
                  }
               }
            } else {
               EntryPosition position = index.getPosition(key, marshaller.objectToByteBuffer(key));
               return position != null;
            }
         }
      } catch (Exception e) {
         throw new PersistenceException("Cannot load key from index", e);
      }
   }

   @Override
   public MarshalledEntry load(Object key) {
      try {
         byte[] serializedValue;
         byte[] serializedKey;
         byte[] serializedMetadata;
         for (;;) {
            EntryPosition entry = temporaryTable.get(key);
            if (entry != null) {
               if (entry.offset < 0) {
                  return null;
               }
               FileProvider.Handle handle = fileProvider.getFile(entry.file);
               if (handle != null) {
                  try {
                     EntryHeader header = EntryRecord.readEntryHeader(handle, entry.offset);
                     if (header == null) {
                        throw new IllegalStateException("Error reading from " + entry.file + ":" + entry.offset + " | " + handle.getFileSize());
                     }
                     if (header.expiryTime() > 0 && header.expiryTime() <= timeService.wallClockTime()) {
                        return null;
                     }
                     serializedKey = EntryRecord.readKey(handle, header, entry.offset);
                     if (header.metadataLength() > 0) {
                        serializedMetadata = EntryRecord.readMetadata(handle, header, entry.offset);
                     } else {
                        serializedMetadata = null;
                     }
                     if (header.valueLength() > 0) {
                        serializedValue = EntryRecord.readValue(handle, header, entry.offset);
                     } else {
                        return null;
                     }
                     return marshalledEntryFactory.newMarshalledEntry(toBuffer(serializedKey), toBuffer(serializedValue), toBuffer(serializedMetadata));
                  } finally {
                     handle.close();
                  }
               }
            } else {
               EntryRecord record = index.getRecord(key, marshaller.objectToByteBuffer(key));
               if (record == null) return null;
               return marshalledEntryFactory.newMarshalledEntry(toBuffer(record.getKey()), toBuffer(record.getValue()), toBuffer(record.getMetadata()));
            }
         }
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   /**
    * This method should be called by reflection to get more info about the missing/invalid key (from test tools)
    * @param key
    * @return
    */
   public String debugInfo(Object key) {
      EntryPosition entry = temporaryTable.get(key);
      if (entry != null) {
         return "temporaryTable: " + entry;
      } else {
         try {
            entry = index.getPosition(key, marshaller.objectToByteBuffer(key));
            return "index: " + entry;
         } catch (Exception e) {
            log.debug("Cannot debug key " + key, e);
            return "exception: " + e;
         }
      }
   }

   private ByteBuffer toBuffer(byte[] array) {
      return array == null ? null : byteBufferFactory.newByteBuffer(array, 0, array.length);
   }

   private interface EntryFunctor {
      boolean apply(int file, int offset, int size, byte[] serializedKey, byte[] serializedMetadata, byte[] serializedValue, long seqId, long expiration) throws Exception;
   }

   private interface FileFunctor {
      void afterFile(int file);
   }

   private void forEachOnDisk(boolean readMetadata, boolean readValues, EntryFunctor functor, FileFunctor fileFunctor) throws PersistenceException {
      try {
         Iterator<Integer> iterator = fileProvider.getFileIterator();
         while (iterator.hasNext()) {
            int file = iterator.next();
            log.debug("Loading entries from file " + file);
            FileProvider.Handle handle = fileProvider.getFile(file);
            if (handle == null) {
               log.debug("File " + file + " was deleted during iteration");
               fileFunctor.afterFile(file);
               continue;
            }
            try {
               int offset = 0;
               for (;;) {
                  EntryHeader header = EntryRecord.readEntryHeader(handle, offset);
                  if (header == null) {
                     break; // end of file;
                  }
                  try {
                     byte[] serializedKey = EntryRecord.readKey(handle, header, offset);
                     if (serializedKey == null) {
                        break; // we have read the file concurrently with writing there
                        //throw new CacheLoaderException("File " + file + " appears corrupt when reading key from " + offset + ": header is " + header);
                     }
                     byte[] serializedMetadata = null;
                     if (readMetadata && header.metadataLength() > 0) {
                        serializedMetadata = EntryRecord.readMetadata(handle, header, offset);
                     }
                     byte[] serializedValue = null;
                     int offsetOrNegation = offset;
                     if (header.valueLength() > 0) {
                        if (header.expiryTime() >= 0 && header.expiryTime() <= timeService.wallClockTime()) {
                           offsetOrNegation = ~offset;
                        } else if (readValues) {
                           serializedValue = EntryRecord.readValue(handle, header, offset);
                        }
                     } else {
                        offsetOrNegation = ~offset;
                     }
                     if (!functor.apply(file, offsetOrNegation, header.totalLength(), serializedKey, serializedMetadata, serializedValue, header.seqId(), header.expiryTime())) {
                        return;
                     }
                  } finally {
                     offset += header.totalLength();
                  }
               }
            } finally {
               handle.close();
               fileFunctor.afterFile(file);
            }
         }
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   @Override
   public void process(KeyFilter filter, final CacheLoaderTask task, final Executor executor, final boolean fetchValue, final boolean fetchMetadata) {
      final TaskContext context = new TaskContextImpl();
      final KeyFilter notNullFilter = PersistenceUtil.notNull(filter);
      final AtomicLong tasksSubmitted = new AtomicLong();
      final AtomicLong tasksFinished = new AtomicLong();
      forEachOnDisk(fetchMetadata, fetchValue, new EntryFunctor() {
         @Override
         public boolean apply(int file, int offset, int size,
                              final byte[] serializedKey, final byte[] serializedMetadata, final byte[] serializedValue,
                              long seqId, long expiration) throws IOException, ClassNotFoundException {
            if (context.isStopped()) {
               return false;
            }
            final Object key = marshaller.objectFromByteBuffer(serializedKey);
            if (!notNullFilter.accept(key)) {
               return true;
            }
            EntryPosition entry = temporaryTable.get(key);
            if (entry == null) {
               entry = index.getPosition(key, serializedKey);
            }
            if (entry != null && entry.offset >= 0) {
               FileProvider.Handle handle = fileProvider.getFile(entry.file);
               try {
                  EntryHeader header = EntryRecord.readEntryHeader(handle, entry.offset);
                  if (header == null) {
                     throw new IllegalStateException("Cannot read " + entry.file + ":" + entry.offset);
                  }
                  if (seqId < header.seqId()) {
                     return true;
                  }
               } finally {
                  handle.close();
               }
            } else {
               // entry is not in index = it was deleted
               return true;
            }
            if (serializedValue != null && (expiration < 0 || expiration > timeService.wallClockTime())) {
               executor.execute(new Runnable() {
                  @Override
                  public void run() {
                     try {
                        task.processEntry(marshalledEntryFactory.newMarshalledEntry(key,
                              serializedValue == null ? null : marshaller.objectFromByteBuffer(serializedValue),
                              serializedMetadata == null ? null : (InternalMetadata) marshaller.objectFromByteBuffer(serializedMetadata)),
                              context);
                     } catch (Exception e) {
                        log.error("Failed to process task for key " + key, e);
                     } finally {
                        long finished = tasksFinished.incrementAndGet();
                        if (finished == tasksSubmitted.longValue()) {
                           synchronized (context) {
                              context.notifyAll();
                           }
                        }
                     }
                  }
               });
               tasksSubmitted.incrementAndGet();
               return !context.isStopped();
            }
            return true;
         }
      }, new FileFunctor() {
         @Override
         public void afterFile(int file) {
            // noop
         }
      });
      while (tasksSubmitted.longValue() > tasksFinished.longValue()) {
         synchronized (context) {
            try {
               context.wait(100);
            } catch (InterruptedException e) {
               log.error("Iteration was interrupted", e);
               Thread.currentThread().interrupt();
               return;
            }
         }
      }
   }
}
