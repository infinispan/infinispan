package org.infinispan.persistence.sifs;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.util.AbstractIterator;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

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
@Store
public class SoftIndexFileStore implements AdvancedLoadWriteStore {

   private static final Log log = LogFactory.getLog(SoftIndexFileStore.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private SoftIndexFileStoreConfiguration configuration;
   private boolean started = false;
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
   private int maxKeyLength;

   @Override
   public void init(InitializationContext ctx) {
      configuration = ctx.getConfiguration();
      marshaller = ctx.getMarshaller();
      marshalledEntryFactory = ctx.getMarshalledEntryFactory();
      byteBufferFactory = ctx.getByteBufferFactory();
      timeService = ctx.getTimeService();
      maxKeyLength = configuration.maxNodeSize() - IndexNode.RESERVED_SPACE;
   }

   @Override
   public void start() {
      if (started) {
         throw new IllegalStateException("This store is already started!");
      }
      started = true;
      temporaryTable = new TemporaryTable(configuration.indexQueueLength() * configuration.indexSegments());
      storeQueue = new SyncProcessingQueue<>();
      indexQueue = new IndexQueue(configuration.indexSegments(), configuration.indexQueueLength());
      fileProvider = new FileProvider(configuration.dataLocation(), configuration.openFilesLimit());
      compactor = new Compactor(fileProvider, temporaryTable, indexQueue, marshaller, timeService, configuration.maxFileSize(), configuration.compactionThreshold());
      logAppender = new LogAppender(storeQueue, indexQueue, temporaryTable, compactor, fileProvider, configuration.syncWrites(), configuration.maxFileSize());
      try {
         index = new Index(fileProvider, configuration.indexLocation(), configuration.indexSegments(),
               configuration.minNodeSize(), configuration.maxNodeSize(),
               indexQueue, temporaryTable, compactor, timeService);
      } catch (IOException e) {
         throw log.cannotOpenIndex(configuration.indexLocation(), e);
      }
      compactor.setIndex(index);
      startIndex();
      final AtomicLong maxSeqId = new AtomicLong(0);
      if (index.isLoaded()) {
         log.debug("Not building the index - loaded from persisted state");
      } else if (configuration.purgeOnStartup()) {
         log.debug("Not building the index - purge will be executed");
      } else {
         log.debug("Building the index");

         Flowable<Integer> filePublisher = filePublisher();
         handleFilePublisher(filePublisher.doAfterNext(compactor::completeFile), false, false,
               (file, offset, size, serializedKey, serializedMetadata, serializedValue, seqId, expiration) -> {
                  long prevSeqId;
                  while (seqId > (prevSeqId = maxSeqId.get()) && !maxSeqId.compareAndSet(prevSeqId, seqId)) {
                  }
                  Object key = marshaller.objectFromByteBuffer(serializedKey);
                  if (trace) {
                     log.tracef("Loaded %d:%d (seqId %d, expiration %d)", file, offset, seqId, expiration);
                  }
                  try {
                     // We may check the seqId safely as we are the only thread writing to index
                     if (isSeqIdOld(seqId, key, serializedKey)) {
                        indexQueue.put(IndexRequest.foundOld(key, serializedKey, file, offset));
                        return null;
                     }
                     temporaryTable.set(key, file, offset);
                     indexQueue.put(IndexRequest.update(key, serializedKey, file, offset, size));
                  } catch (InterruptedException e) {
                     log.error("Interrupted building of index, the index won't be built properly!", e);
                     return null;
                  }
                  return null;
               }).blockingSubscribe();
      }
      logAppender.setSeqId(maxSeqId.get() + 1);
   }

   protected boolean isSeqIdOld(long seqId, Object key, byte[] serializedKey) throws IOException {
      for (; ; ) {
         EntryPosition entry = temporaryTable.get(key);
         if (entry == null) {
            entry = index.getInfo(key, serializedKey);
         }
         if (entry == null) {
            if (trace) {
               log.tracef("Did not found position for %s", key);
            }
            return false;
         } else {
            FileProvider.Handle handle = fileProvider.getFile(entry.file);
            if (handle == null) {
               // the file was deleted after we've looked up temporary table/index
               continue;
            }
            try {
               int entryOffset = entry.offset < 0 ? ~entry.offset : entry.offset;
               EntryHeader header = EntryRecord.readEntryHeader(handle, entryOffset);
               if (header == null) {
                  throw new IOException("Cannot read " + entry.file + ":" + entryOffset);
               }
               if (trace) {
                  log.tracef("SeqId on %d:%d is %d", entry.file, entry.offset, header.seqId());
               }
               return seqId < header.seqId();
            } finally {
               handle.close();
            }
         }
      }
   }

   protected void startIndex() {
      // this call is extracted for better testability
      index.start();
   }

   protected boolean isIndexLoaded() {
      return index.isLoaded();
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
         Thread.currentThread().interrupt();
         throw log.interruptedWhileStopping(e);
      } finally {
         started = false;
      }
   }

   @Override
   public synchronized void destroy() {
      try {
         logAppender.stopOperations();
         logAppender = null;
         compactor.stopOperations();
         compactor = null;
         try {
            index.clear();
         } catch (IOException e) {
            log.debug("Couldn't clear index", e);
         }
         index.stopOperations();
         index = null;
         try {
            fileProvider.clear();
         } catch (IOException e) {
            log.debug("Couldn't clear fileProvider", e);
         }
         fileProvider = null;
         temporaryTable = null;
         indexQueue = null;
         storeQueue = null;
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw log.interruptedWhileStopping(e);
      } finally {
         started = false;
      }
   }

   @Override
   public boolean isAvailable() {
      return new File(configuration.dataLocation()).exists() && new File(configuration.dataLocation()).exists();
   }

   @Override
   public synchronized void clear() throws PersistenceException {
      try {
         logAppender.clearAndPause();
         compactor.clearAndPause();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw log.interruptedWhileClearing(e);
      }
      try {
         index.clear();
      } catch (IOException e) {
         throw log.cannotClearIndex(e);
      }
      try {
         fileProvider.clear();
      } catch (IOException e) {
         throw log.cannotClearData(e);
      }
      temporaryTable.clear();
      compactor.resumeAfterPause();
      logAppender.resumeAfterPause();
   }

   @Override
   public synchronized int size() {
      try {
         logAppender.pause();
         long size = index.size();
         return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw log.sizeCalculationInterrupted(e);
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
         throw log.keyIsTooLong(entry.getKey(), keyLength, configuration.maxNodeSize(), maxKeyLength);
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
         throw log.cannotLoadKeyFromIndex(key, e);
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
                  log.tracef("Entry for key=%s found in temporary table on %d:%d but it is a tombstone", key, entry.file, entry.offset);
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
                        if (trace) {
                           log.tracef("Entry for key=%s found in temporary table on %d:%d but it is expired", key, entry.file, entry.offset);
                        }
                        return null;
                     }
                     serializedKey = EntryRecord.readKey(handle, header, entry.offset);
                     if (serializedKey == null) {
                        throw new IllegalStateException("Error reading key from "  + entry.file + ":" + entry.offset);
                     }
                     if (header.metadataLength() > 0) {
                        serializedMetadata = EntryRecord.readMetadata(handle, header, entry.offset);
                     } else {
                        serializedMetadata = null;
                     }
                     if (header.valueLength() > 0) {
                        serializedValue = EntryRecord.readValue(handle, header, entry.offset);
                        if (trace) {
                           log.tracef("Entry for key=%s found in temporary table on %d:%d and loaded", key, entry.file, entry.offset);
                        }
                     } else {
                        if (trace) {
                           log.tracef("Entry for key=%s found in temporary table on %d:%d but it is a tombstone in log", key, entry.file, entry.offset);
                        }
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
         throw log.cannotLoadKeyFromIndex(key, e);
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
            log.debugf(e, "Cannot debug key %s", key);
            return "exception: " + e;
         }
      }
   }

   private ByteBuffer toBuffer(byte[] array) {
      return array == null ? null : byteBufferFactory.newByteBuffer(array, 0, array.length);
   }

   private interface EntryFunctor<R> {
      R apply(int file, int offset, int size, byte[] serializedKey, byte[] serializedMetadata, byte[] serializedValue, long seqId, long expiration) throws Exception;
   }

   private Flowable<Integer> filePublisher() {
      return Flowable.using(fileProvider::getFileIterator, it -> Flowable.fromIterable(() -> it),
            // This close happens after the lasst file iterator is returned, but before processing it.
            // TODO: Is this okay or can compaction etc affect this?
            CloseableIterator::close);
   }

   private <R> Flowable<R> handleFilePublisher(Flowable<Integer> filePublisher, boolean fetchValue, boolean fetchMetadata,
         EntryFunctor<R> functor) {
      return filePublisher.flatMap(f -> {
         // Unbox here once
         int file = f;
         return Flowable.using(() -> {
                  log.debugf("Loading entries from file %d", file);
                  return Optional.ofNullable(fileProvider.getFile(file));
               },
               optHandle -> {
                  if (!optHandle.isPresent()) {
                     log.debugf("File %d was deleted during iteration", file);
                     return Flowable.empty();
                  }

                  FileProvider.Handle handle = optHandle.get();
                  AtomicInteger offset = new AtomicInteger();
                  return Flowable.fromIterable(() -> new HandleIterator<>(offset, handle, fetchMetadata, fetchValue,
                        functor, file));
               },
               optHandle -> {
                  if (optHandle.isPresent()) {
                     optHandle.get().close();
                  }
               }
         );
      });
   }

   @Override
   public Publisher publishKeys(Predicate filter) {
      return handleFilePublisher(filePublisher(), false, true,
            (file, offset, size, serializedKey, serializedMetadata, serializedValue, seqId, expiration) -> {

               final Object key = marshaller.objectFromByteBuffer(serializedKey);

               if (serializedValue != null && (filter == null || filter.test(key)) && !isSeqIdOld(seqId, key, serializedKey)) {
                  return key;
               }
               return null;
            });
   }

   @Override
   public Publisher<MarshalledEntry> publishEntries(Predicate filter, boolean fetchValue, boolean fetchMetadata) {
      return handleFilePublisher(filePublisher(), fetchValue, fetchMetadata,
            (file, offset, size, serializedKey, serializedMetadata, serializedValue, seqId, expiration) -> {

               final Object key = marshaller.objectFromByteBuffer(serializedKey);

               // SerializedValue is tested to handle when a remove is found
               if (serializedValue != null && (filter == null || filter.test(key)) && !isSeqIdOld(seqId, key, serializedKey)) {
                  return marshalledEntryFactory.newMarshalledEntry(key,
                        // EMPTY_BYTES is used to symbolize when fetchValue is false but there was an entry
                        serializedValue != Util.EMPTY_BYTE_ARRAY ? marshaller.objectFromByteBuffer(serializedValue) : null,
                        serializedMetadata == null ? null : (InternalMetadata) marshaller.objectFromByteBuffer(serializedMetadata));
               }
               return null;
            });
   }

   private class HandleIterator<R> extends AbstractIterator<R> {
      private final AtomicInteger offset;
      private final FileProvider.Handle handle;
      private final boolean fetchMetadata;
      private final boolean fetchValue;
      private final EntryFunctor<R> functor;
      private final int file;

      public HandleIterator(AtomicInteger offset, FileProvider.Handle handle, boolean fetchMetadata, boolean fetchValue,
            EntryFunctor<R> functor, int file) {
         this.offset = offset;
         this.handle = handle;
         this.fetchMetadata = fetchMetadata;
         this.fetchValue = fetchValue;
         this.functor = functor;
         this.file = file;
      }

      @Override
      protected R getNext() {
         R next = null;
         int innerOffset = offset.get();
         try {
            while (next == null) {
               EntryHeader header = EntryRecord.readEntryHeader(handle, innerOffset);
               if (header == null) {
                  return null; // end of file;
               }
               try {
                  byte[] serializedKey = EntryRecord.readKey(handle, header, innerOffset);
                  if (serializedKey == null) {
                     continue; // we have read the file concurrently with writing there
                  }
                  byte[] serializedMetadata = null;
                  if (fetchMetadata && header.metadataLength() > 0) {
                     serializedMetadata = EntryRecord.readMetadata(handle, header, innerOffset);
                  }
                  byte[] serializedValue = null;
                  int offsetOrNegation = innerOffset;
                  if (header.valueLength() > 0) {
                     if (header.expiryTime() >= 0 && header.expiryTime() <= timeService.wallClockTime()) {
                        offsetOrNegation = ~innerOffset;
                     } else if (fetchValue) {
                        serializedValue = EntryRecord.readValue(handle, header, innerOffset);
                     } else {
                        serializedValue = Util.EMPTY_BYTE_ARRAY;
                     }
                  } else {
                     offsetOrNegation = ~innerOffset;
                  }

                  next = functor.apply(file, offsetOrNegation, header.totalLength(), serializedKey,
                        serializedMetadata, serializedValue, header.seqId(), header.expiryTime());
               } finally {
                  innerOffset = offset.addAndGet(header.totalLength());
               }
            }
            return next;
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }
   }
}
