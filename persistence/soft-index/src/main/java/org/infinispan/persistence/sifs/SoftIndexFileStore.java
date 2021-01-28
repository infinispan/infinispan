package org.infinispan.persistence.sifs;

import static org.infinispan.persistence.PersistenceUtil.getQualifiedLocation;
import static org.infinispan.util.logging.Log.PERSISTENCE;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.AbstractIterator;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Util;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

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
public class SoftIndexFileStore implements AdvancedLoadWriteStore<Object, Object> {

   private static final Log log = LogFactory.getLog(SoftIndexFileStore.class, Log.class);

   public static final String PREFIX_10_1 = "";
   public static final String PREFIX_11_0 = "ispn.";
   public static final String PREFIX_12_0 = "ispn12.";
   public static final String PREFIX_LATEST = PREFIX_12_0;

   private SoftIndexFileStoreConfiguration configuration;
   private boolean started = false;
   private TemporaryTable temporaryTable;
   private IndexQueue indexQueue;
   private SyncProcessingQueue<LogRequest> storeQueue;
   private FileProvider fileProvider;
   private LogAppender logAppender;
   private Index index;
   private Compactor compactor;
   private Marshaller marshaller;
   private ByteBufferFactory byteBufferFactory;
   private MarshallableEntryFactory<Object, Object> marshallableEntryFactory;
   private TimeService timeService;
   private int maxKeyLength;
   private InitializationContext ctx;

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      configuration = ctx.getConfiguration();
      marshaller = ctx.getPersistenceMarshaller();
      marshallableEntryFactory = ctx.getMarshallableEntryFactory();
      byteBufferFactory = ctx.getByteBufferFactory();
      timeService = ctx.getTimeService();
      maxKeyLength = configuration.maxNodeSize() - IndexNode.RESERVED_SPACE;
   }

   @Override
   public synchronized void start() {
      if (started) {
         throw new IllegalStateException("This store is already started!");
      }
      started = true;
      temporaryTable = new TemporaryTable(configuration.indexQueueLength() * configuration.indexSegments());
      storeQueue = new SyncProcessingQueue<>();
      indexQueue = new IndexQueue(configuration.indexSegments(), configuration.indexQueueLength());
      fileProvider = new FileProvider(getDataLocation(), configuration.openFilesLimit(), PREFIX_LATEST);
      compactor = new Compactor(fileProvider, temporaryTable, indexQueue, marshaller, timeService, configuration.maxFileSize(), configuration.compactionThreshold());
      logAppender = new LogAppender(storeQueue, indexQueue, temporaryTable, compactor, fileProvider, configuration.syncWrites(), configuration.maxFileSize());
      try {
         index = new Index(fileProvider, getIndexLocation(), configuration.indexSegments(),
               configuration.minNodeSize(), configuration.maxNodeSize(),
               indexQueue, temporaryTable, compactor, timeService);
      } catch (IOException e) {
         throw log.cannotOpenIndex(configuration.indexLocation(), e);
      }
      compactor.setIndex(index);
      startIndex();
      final AtomicLong maxSeqId = new AtomicLong(0);
      boolean migrateData = false;

      if (!configuration.purgeOnStartup()) {
         // we don't destroy the data on startup
         // get the old files
         FileProvider oldFileProvider = new FileProvider(getDataLocation(), configuration.openFilesLimit(), PREFIX_10_1);
         if (oldFileProvider.hasFiles()) {
            throw PERSISTENCE.persistedDataMigrationAcrossMajorVersions();
         }
         oldFileProvider = new FileProvider(getDataLocation(), configuration.openFilesLimit(), PREFIX_11_0);
         if (oldFileProvider.hasFiles()) {
            migrateFromOldFormat(oldFileProvider);
            migrateData = true;
         } else if (index.isLoaded()) {
            log.debug("Not building the index - loaded from persisted state");
         } else {
            log.debug("Building the index");
            buildIndex(maxSeqId);
         }
      } else {
         log.debug("Not building the index - purge will be executed");
      }
      if (!migrateData) {
         logAppender.setSeqId(maxSeqId.get() + 1);
      }
   }

   private void migrateFromOldFormat(FileProvider oldFileProvider) {
      String cacheName = ctx.getCache().getName();
      PERSISTENCE.startMigratingPersistenceData(cacheName);
      try {
         index.clear();
      } catch (IOException e) {
         throw PERSISTENCE.persistedDataMigrationFailed(cacheName, e);
      }
      // Only update the key/value/meta bytes if the default marshaller is configured
      boolean transformationRequired = ctx.getGlobalConfiguration().serialization().marshaller() == null;
      try(CloseableIterator<Integer> it = oldFileProvider.getFileIterator()) {
         while (it.hasNext()) {
            int fileId = it.next();
            try (FileProvider.Handle handle = oldFileProvider.getFile(fileId)) {
               int offset = 0;
               while (true) {
                  EntryHeader header = EntryRecord.readEntryHeader(handle, offset);
                  if (header == null) {
                     //end of file. go to next one
                     break;
                  }
                  MarshallableEntry<Object, Object> entry = readEntry(handle, header, offset, null, true,
                        (key, value, meta, internalMeta, created, lastUsed) -> {
                           if (!transformationRequired) {
                              return marshallableEntryFactory.create(key, value, meta, internalMeta, created, lastUsed);
                           }
                           try {
                              Object k = unmarshallLegacy(key, false);
                              Object v = unmarshallLegacy(value, false);
                              Metadata m = unmarshallLegacy(meta, true);
                              PrivateMetadata im = internalMeta == null ? null : (PrivateMetadata) ctx.getPersistenceMarshaller().objectFromByteBuffer(internalMeta.getBuf());
                              return marshallableEntryFactory.create(k, v, m, im, created, lastUsed);
                           } catch (ClassNotFoundException | IOException e) {
                              throw new MarshallingException(e);
                           }
                        });
                  // entry is null if expired or removed (tombstone), in both case, we can ignore it.
                  //noinspection ConstantConditions (entry is not null!)
                  if (entry.getValueBytes() != null) {
                     // using the storeQueue (instead of binary copy) to avoid building the index later
                     storeQueue.pushAndWait(LogRequest.storeRequest(entry));
                  } else {
                     // delete the entry. The file is append only so we can have a put() and later a remove() for the same key
                     storeQueue.pushAndWait(LogRequest.deleteRequest(entry.getKey(), entry.getKeyBytes()));
                  }
                  offset += header.totalLength();
               }
            }
            // file is read. can be removed.
            oldFileProvider.deleteFile(fileId);
         }
         PERSISTENCE.persistedDataSuccessfulMigrated(cacheName);
      } catch (InterruptedException | IOException e) {
         throw PERSISTENCE.persistedDataMigrationFailed(cacheName, e);
      }
   }

   private <T> T unmarshallLegacy(ByteBuffer buf, boolean allowInternal) throws ClassNotFoundException, IOException {
      if (buf == null)
         return null;
      // Read using raw user marshaller without MarshallUserObject wrapping
      Marshaller marshaller = ctx.getPersistenceMarshaller().getUserMarshaller();
      try {
         return (T) marshaller.objectFromByteBuffer(buf.getBuf(), buf.getOffset(), buf.getLength());
      } catch (IllegalArgumentException e) {
         // For metadata we need to attempt to read with user-marshaller first in case custom metadata used, otherwise use the persistence marshaller
         if (allowInternal) {
            return (T) ctx.getPersistenceMarshaller().objectFromByteBuffer(buf.getBuf(), buf.getOffset(), buf.getLength());
         }
         throw e;
      }
   }

   private void buildIndex(final AtomicLong maxSeqId) {
      Flowable<Integer> filePublisher = filePublisher();
      CompletionStage<Void> stage = handleFilePublisher(filePublisher.doAfterNext(compactor::completeFile), false, false,
            (file, offset, size, serializedKey, entryMetadata, serializedValue, serializedInternalMetadata, seqId, expiration) -> {
               long prevSeqId;
               while (seqId > (prevSeqId = maxSeqId.get()) && !maxSeqId.compareAndSet(prevSeqId, seqId)) {
               }
               Object key = marshaller.objectFromByteBuffer(serializedKey);
               if (log.isTraceEnabled()) {
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
            }).ignoreElements()
            .toCompletionStage(null);
      CompletionStages.join(stage);
   }

   private Path getDataLocation() {
      return getQualifiedLocation(ctx.getGlobalConfiguration(), configuration.dataLocation(), ctx.getCache().getName(), "data");
   }

   protected Path getIndexLocation() {
      return getQualifiedLocation(ctx.getGlobalConfiguration(), configuration.indexLocation(), ctx.getCache().getName(), "index");
   }

   protected boolean isSeqIdOld(long seqId, Object key, byte[] serializedKey) throws IOException {
      for (; ; ) {
         EntryPosition entry = temporaryTable.get(key);
         if (entry == null) {
            entry = index.getInfo(key, serializedKey);
         }
         if (entry == null) {
            if (log.isTraceEnabled()) {
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
               if (log.isTraceEnabled()) {
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
   public synchronized void stop() {
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
   public void purge(Executor threadPool, PurgeListener<? super Object> listener) {
      log.trace("Purge method not supported, ignoring.");
      // TODO: in future we may support to force compactor run on all files
   }

   @Override
   public void write(MarshallableEntry<?, ?> entry) {
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
   public MarshallableEntry<Object, Object> loadEntry(Object key) {
      try {
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
                     return readEntry(handle, header, entry.offset, key, false,
                           (serializedKey, value, meta, internalMeta, created, lastUsed) ->
                                 marshallableEntryFactory.create(serializedKey, value, meta, internalMeta, created, lastUsed));
                  } finally {
                     handle.close();
                  }
               }
            } else {
               EntryRecord record = index.getRecord(key, marshaller.objectToByteBuffer(key));
               if (record == null) return null;
               return marshallableEntryFactory.create(toBuffer(record.getKey()), toBuffer(record.getValue()),
                     toBuffer(record.getMetadata()), toBuffer(record.getInternalMetadata()), record.getCreated(), record.getLastUsed());
            }
         }
      } catch (Exception e) {
         throw log.cannotLoadKeyFromIndex(key, e);
      }
   }

   private MarshallableEntry<Object, Object> readEntry(FileProvider.Handle handle, EntryHeader header, int offset,
                                                       Object key, boolean nonNull, EntryCreator<Object, Object> entryCreator)
         throws IOException {
      if (header.expiryTime() > 0 && header.expiryTime() <= timeService.wallClockTime()) {
         if (log.isTraceEnabled()) {
            log.tracef("Entry for key=%s found in temporary table on %d:%d but it is expired", key, handle.getFileId(), offset);
         }
         return nonNull ?
               entryCreator.create(readAndCheckKey(handle, header, offset), null, null, null, -1, -1) :
               null;
      }
      ByteBuffer serializedKey = readAndCheckKey(handle, header, offset);
      if (header.valueLength() <= 0) {
         if (log.isTraceEnabled()) {
            log.tracef("Entry for key=%s found in temporary table on %d:%d but it is a tombstone in log", key, handle.getFileId(), offset);
         }
         return nonNull ? entryCreator.create(serializedKey, null, null, null, -1, -1) : null;
      }

      if (log.isTraceEnabled()) {
         log.tracef("Entry for key=%s found in temporary table on %d:%d and loaded", key, handle.getFileId(), offset);
      }

      ByteBuffer value = toBuffer(EntryRecord.readValue(handle, header, offset));
      ByteBuffer serializedMetadata;
      long created;
      long lastUsed;
      if (header.metadataLength() > 0) {
         EntryMetadata metadata = EntryRecord.readMetadata(handle, header, offset);
         serializedMetadata = toBuffer(metadata.getBytes());
         created = metadata.getCreated();
         lastUsed = metadata.getLastUsed();
      } else {
         serializedMetadata = null;
         created = -1;
         lastUsed = -1;
      }

      ByteBuffer internalMetadata = header.internalMetadataLength() > 0 ?
            toBuffer(EntryRecord.readInternalMetadata(handle, header, offset)) :
            null;

      return entryCreator.create(serializedKey, value, serializedMetadata, internalMetadata, created, lastUsed);
   }

   interface EntryCreator<K,V> {
      MarshallableEntry<K, V> create(ByteBuffer key, ByteBuffer value, ByteBuffer metadata,
                                            ByteBuffer internalMetadata, long created, long lastUsed) throws IOException;
   }

   private ByteBuffer readAndCheckKey(FileProvider.Handle handle, EntryHeader header, int offset) throws IOException {
      ByteBuffer serializedKey = toBuffer(EntryRecord.readKey(handle, header, offset));
      if (serializedKey == null) {
         throw new IllegalStateException("Error reading key from "  + handle.getFileId() + ":" + offset);
      }
      return serializedKey;
   }

   private ByteBuffer toBuffer(byte[] array) {
      return array == null ? null : byteBufferFactory.newByteBuffer(array, 0, array.length);
   }

   private interface EntryFunctor<R> {
      R apply(int file, int offset, int size, byte[] serializedKey, EntryMetadata metadata, byte[] serializedValue, byte[] serializedInternalMetadata, long seqId, long expiration) throws Exception;
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
   public Publisher<Object> publishKeys(Predicate<? super Object> filter) {
      return handleFilePublisher(filePublisher(), false, true,
            (file, offset, size, serializedKey, entryMetadata, serializedValue, serializedInternalMetadata, seqId, expiration) -> {

               final Object key = marshaller.objectFromByteBuffer(serializedKey);

               if (serializedValue != null && (filter == null || filter.test(key)) && !isSeqIdOld(seqId, key, serializedKey)) {
                  return key;
               }
               return null;
            });
   }

   @Override
   public Publisher<MarshallableEntry<Object, Object>> entryPublisher(Predicate<? super Object> filter, boolean fetchValue, boolean fetchMetadata) {
      return handleFilePublisher(filePublisher(), fetchValue, fetchMetadata,
            (file, offset, size, serializedKey, entryMetadata, serializedValue, serializedInternalMetadata, seqId, expiration) -> {

               final Object key = marshaller.objectFromByteBuffer(serializedKey);

               // SerializedValue is tested to handle when a remove is found
               if (serializedValue != null && (filter == null || filter.test(key)) && !isSeqIdOld(seqId, key, serializedKey)) {
                  // EMPTY_BYTES is used to symbolize when fetchValue is false but there was an entry
                  final Object value = serializedValue == Util.EMPTY_BYTE_ARRAY ? null : marshaller.objectFromByteBuffer(serializedValue);
                  PrivateMetadata internalMetadata = serializedInternalMetadata == null ?
                        null :
                        (PrivateMetadata) marshaller.objectFromByteBuffer(serializedInternalMetadata);
                  if (entryMetadata == null)
                     return marshallableEntryFactory.create(key, value, null, internalMetadata, -1, -1);

                  final Metadata metadata = (Metadata) marshaller.objectFromByteBuffer(entryMetadata.getBytes());
                  return marshallableEntryFactory.create(key, value, metadata, internalMetadata, entryMetadata.getCreated(), entryMetadata.getLastUsed());
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
                  EntryMetadata meta = null;
                  if (fetchMetadata && header.metadataLength() > 0) {
                     meta = EntryRecord.readMetadata(handle, header, innerOffset);
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
                  byte[] serializedInternalMetadata = null;
                  if (fetchMetadata && header.internalMetadataLength() > 0) {
                     serializedInternalMetadata = EntryRecord.readInternalMetadata(handle, header, innerOffset);
                  }

                  next = functor.apply(file, offsetOrNegation, header.totalLength(), serializedKey, meta,
                        serializedValue, serializedInternalMetadata, header.seqId(), header.expiryTime());
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
