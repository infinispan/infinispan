package org.infinispan.persistence.sifs;

import static org.infinispan.persistence.PersistenceUtil.getQualifiedLocation;
import static org.infinispan.util.logging.Log.PERSISTENCE;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.AbstractIterator;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.concurrent.ActionSequencer;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;

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
@ConfiguredBy(SoftIndexFileStoreConfigurationBuilder.class)
public class NonBlockingSoftIndexFileStore<K, V> implements NonBlockingStore<K, V> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   public static final String PREFIX_10_1 = "";
   public static final String PREFIX_11_0 = "ispn.";
   public static final String PREFIX_12_0 = "ispn12.";
   public static final String PREFIX_LATEST = PREFIX_12_0;

   private SoftIndexFileStoreConfiguration configuration;
   private TemporaryTable temporaryTable;
   private FileProvider fileProvider;
   private LogAppender logAppender;
   private Index index;
   private Compactor compactor;
   private Marshaller marshaller;
   private ByteBufferFactory byteBufferFactory;
   private MarshallableEntryFactory<K, V> marshallableEntryFactory;
   private TimeService timeService;
   private int maxKeyLength;
   private BlockingManager blockingManager;
   private ActionSequencer sizeAndClearSequencer;
   private KeyPartitioner keyPartitioner;
   private InitializationContext ctx;

   @Override
   public Set<Characteristic> characteristics() {
      return EnumSet.of(Characteristic.BULK_READ, Characteristic.SEGMENTABLE, Characteristic.EXPIRATION);
   }

   @Override
   public CompletionStage<Void> addSegments(IntSet segments) {
      temporaryTable.addSegments(segments);
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> removeSegments(IntSet segments) {
      temporaryTable.removeSegments(segments);
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      this.ctx = ctx;

      keyPartitioner = ctx.getKeyPartitioner();
      blockingManager = ctx.getBlockingManager();
      // TODO: I don't think we need to use blocking executor here
      sizeAndClearSequencer = new ActionSequencer(blockingManager.asExecutor("SIFS-sizeOrClear"),
            false, timeService);
      configuration = ctx.getConfiguration();
      marshaller = ctx.getPersistenceMarshaller();
      marshallableEntryFactory = ctx.getMarshallableEntryFactory();
      byteBufferFactory = ctx.getByteBufferFactory();
      timeService = ctx.getTimeService();
      maxKeyLength = configuration.maxNodeSize() - IndexNode.RESERVED_SPACE;

      Configuration cacheConfig = ctx.getCache().getCacheConfiguration();
      int cacheSegments = cacheConfig.clustering().hash().numSegments();
      temporaryTable = new TemporaryTable(cacheSegments);
      temporaryTable.addSegments(IntSets.immutableRangeSet(cacheConfig.clustering().hash().numSegments()));

      fileProvider = new FileProvider(getDataLocation(), configuration.openFilesLimit(), PREFIX_LATEST,
            configuration.maxFileSize());
      compactor = new Compactor(ctx.getNonBlockingManager(), fileProvider, temporaryTable, marshaller, timeService,
            keyPartitioner, configuration.maxFileSize(), configuration.compactionThreshold(),
            blockingManager.asExecutor("sifs-compactor"));
      try {
         index = new Index(ctx.getNonBlockingManager(), fileProvider, getIndexLocation(), configuration.indexSegments(),
               cacheSegments, configuration.minNodeSize(), configuration.maxNodeSize(), temporaryTable, compactor,
               timeService);
      } catch (IOException e) {
         throw log.cannotOpenIndex(configuration.indexLocation(), e);
      }
      compactor.setIndex(index);
      logAppender = new LogAppender(ctx.getNonBlockingManager(), index, temporaryTable, compactor, fileProvider,
            configuration.syncWrites(), configuration.maxFileSize());
      logAppender.start(blockingManager.asExecutor("sifs-log-processor"));
      startIndex();
      final AtomicLong maxSeqId = new AtomicLong(0);

      return blockingManager.runBlocking(() -> {
         boolean migrateData = false;
         // we don't destroy the data on startup
         // get the old files
         FileProvider oldFileProvider = new FileProvider(getDataLocation(), configuration.openFilesLimit(), PREFIX_10_1,
             configuration.maxFileSize());
         if (oldFileProvider.hasFiles()) {
            throw PERSISTENCE.persistedDataMigrationUnsupportedVersion("< 11");
         }
         oldFileProvider = new FileProvider(getDataLocation(), configuration.openFilesLimit(), PREFIX_11_0,
             configuration.maxFileSize());
         if (oldFileProvider.hasFiles()) {
            try {
               index.reset();
            } catch (IOException e) {
               throw PERSISTENCE.issueEncounteredResettingIndex(ctx.getCache().getName(), e);
            }
            migrateFromOldFormat(oldFileProvider);
            migrateData = true;
         } else if (index.load()) {
            log.debug("Not building the index - loaded from persisted state");
            try {
               maxSeqId.set(index.getMaxSeqId());
            } catch (IOException e) {
               log.debug("Failed to load index. Rebuilding it.");
               buildIndex(maxSeqId);
            }
         } else {
            log.debug("Building the index");
            try {
               index.reset();
            } catch (IOException e) {
               throw PERSISTENCE.issueEncounteredResettingIndex(ctx.getCache().getName(), e);
            }
            buildIndex(maxSeqId);
         }
         if (!migrateData) {
            logAppender.setSeqId(maxSeqId.get() + 1);
         }
         // Compactor may have to write to the index, so it can't be started until after Index has been fully started
         compactor.start();
      }, "soft-index-start");
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> purgeExpired() {
      return Flowable.defer(() -> {
         log.tracef("Purging expired contents from soft index file store");
         UnicastProcessor<MarshallableEntry<K, V>> unicastProcessor = UnicastProcessor.create();
         // Compactor is ran asynchronously
         compactor.performExpirationCompaction(new CompactorSubscriber(unicastProcessor));
         return unicastProcessor;
      });
   }

   private class CompactorSubscriber implements Compactor.CompactionExpirationSubscriber {
      private final FlowableProcessor<MarshallableEntry<K, V>> processor;

      private CompactorSubscriber(FlowableProcessor<MarshallableEntry<K, V>> processor) {
         this.processor = processor;
      }

      @Override
      public void onEntryPosition(EntryPosition entryPosition) throws IOException {
         MarshallableEntry<K, V> entry = readValueFromFileOffset(null, entryPosition, true);
         assert entry != null : "EntryPosition didn't return a value: " + entryPosition;
         processor.onNext(entry);
      }

      @Override
      public void onEntryEntryRecord(EntryRecord entryRecord) {
         MarshallableEntry<K, V> entry = entryFromRecord(entryRecord);
         processor.onNext(entry);
      }

      @Override
      public void onComplete() {
         processor.onComplete();
      }

      @Override
      public void onError(Throwable t) {
         processor.onError(t);
      }
   }

   private void migrateFromOldFormat(FileProvider oldFileProvider) {
      String cacheName = ctx.getCache().getName();
      PERSISTENCE.startMigratingPersistenceData(cacheName);
      try {
         CompletionStages.join(index.clear());
      } catch (CompletionException e) {
         throw PERSISTENCE.persistedDataMigrationFailed(cacheName, e.getCause());
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
                  MarshallableEntry<K, V> entry = readEntry(handle, header, offset, null, true,
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
                        }, false);
                  int segment = keyPartitioner.getSegment(entry.getKey());
                  // entry is null if expired or removed (tombstone), in both case, we can ignore it.
                  //noinspection ConstantConditions (entry is not null!)
                  if (entry.getValueBytes() != null) {
                     // using the storeQueue (instead of binary copy) to avoid building the index later
                     CompletionStages.join(logAppender.storeRequest(segment, entry));
                  } else {
                     // delete the entry. The file is append only so we can have a put() and later a remove() for the same key
                     CompletionStages.join(logAppender.deleteRequest(segment, entry.getKey(), entry.getKeyBytes()));
                  }
                  offset += header.totalLength();
               }
            }
            // file is read. can be removed.
            oldFileProvider.deleteFile(fileId);
         }
         PERSISTENCE.persistedDataSuccessfulMigrated(cacheName);
      } catch (IOException e) {
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
      CompletionStage<Void> stage = filePublisher.flatMap(outerFile -> {
         ByRef.Long nextExpirationTime = new ByRef.Long(-1);
         return handleFilePublisher(outerFile, false, false,
               (file, offset, size, serializedKey, entryMetadata, serializedValue, serializedInternalMetadata, seqId, expiration) -> {
                  long prevSeqId;
                  while (seqId > (prevSeqId = maxSeqId.get()) && !maxSeqId.compareAndSet(prevSeqId, seqId)) {
                  }
                  Object key = marshaller.objectFromByteBuffer(serializedKey);
                  if (log.isTraceEnabled()) {
                     log.tracef("Loaded %d:%d (seqId %d, expiration %d)", file, offset, seqId, expiration);
                  }
                  // Make sure to keep track of the lowest expiration that isn't -1
                  nextExpirationTime.set(ExpiryHelper.mostRecentExpirationTime(nextExpirationTime.get(), expiration));
                  int segment = keyPartitioner.getSegment(key);
                  // We may check the seqId safely as we are the only thread writing to index
                  if (isSeqIdOld(seqId, segment, key, serializedKey)) {
                     index.handleRequest(IndexRequest.foundOld(segment, key, ByteBufferImpl.create(serializedKey), file, offset, size));
                     return null;
                  }
                  if (temporaryTable.set(segment, key, file, offset)) {
                     index.handleRequest(IndexRequest.update(segment, key, ByteBufferImpl.create(serializedKey), false, file, offset, size));
                  }
                  return null;
               }).doOnComplete(() -> compactor.completeFile(outerFile, -1, nextExpirationTime.get(), false));
      }).ignoreElements().toCompletionStage(null);
      CompletionStages.join(stage);
   }

   private Path getDataLocation() {
      return getQualifiedLocation(ctx.getGlobalConfiguration(), configuration.dataLocation(), ctx.getCache().getName(), "data");
   }

   protected Path getIndexLocation() {
      return getQualifiedLocation(ctx.getGlobalConfiguration(), configuration.indexLocation(), ctx.getCache().getName(), "index");
   }

   protected boolean isSeqIdOld(long seqId, int segment, Object key, byte[] serializedKey) throws IOException {
      for (; ; ) {
         EntryPosition entry = temporaryTable.get(segment, key);
         if (entry == null) {
            entry = index.getInfo(key, segment, serializedKey);
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
                  log.tracef("SeqId on %d:%d for key %s is %d", entry.file, entry.offset, key, header.seqId());
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
      index.start(blockingManager.asExecutor("sifs-index"));
   }

   @Override
   public CompletionStage<Void> stop() {
      return blockingManager.runBlocking(() -> {
         try {
            logAppender.stop();
            compactor.stopOperations();
            compactor = null;
            CompletionStages.join(index.stop());
            index = null;
            fileProvider.stop();
            fileProvider = null;
            temporaryTable = null;
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw log.interruptedWhileStopping(e);
         }
      }, "soft-index-stop");
   }

   @Override
   public CompletionStage<Boolean> isAvailable() {
      // TODO: does this block?
      return CompletableFuture.completedFuture(getDataLocation().toFile().exists() &&
            getIndexLocation().toFile().exists());
   }

   @Override
   public CompletionStage<Void> clear() {
      return sizeAndClearSequencer.orderOnKey(this, () -> {
         CompletionStage<Void> chainedStage = logAppender.clearAndPause();
         chainedStage = chainedStage.thenCompose(ignore -> compactor.clearAndPause());
         chainedStage = chainedStage.thenCompose(ignore -> index.clear());

         return blockingManager.thenRunBlocking(chainedStage, () -> {
                  try {
                     fileProvider.clear();
                  } catch (IOException e) {
                     throw log.cannotClearData(e);
                  }
                  temporaryTable.clear();
                  compactor.resumeAfterClear();
               }, "soft-index-file-clear")
               .thenCompose(v -> logAppender.resume());
            }
      );
   }

   @Override
   public CompletionStage<Long> size(IntSet segments) {
      return sizeAndClearSequencer.orderOnKey(this, () ->
            logAppender.pause()
                  .thenCompose(v -> {
                     // Since this is invoked with the logAppender paused it is an exact size
                     long size = index.approximateSize(segments);
                     return logAppender.resume().thenApply(ignore -> size);
                  })
      );
   }

   @Override
   public CompletionStage<Long> approximateSize(IntSet segments) {
      // Approximation doesn't pause the appender so the index can be slightly out of sync
      return CompletableFuture.completedFuture(index.approximateSize(segments));
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      int keyLength = entry.getKeyBytes().getLength();
      if (keyLength > maxKeyLength) {
         return CompletableFutures.completedExceptionFuture(log.keyIsTooLong(entry.getKey(), keyLength, configuration.maxNodeSize(), maxKeyLength));
      }
      try {
         log.tracef("Writing entry for key %s for segment %d", entry.getKey(), segment);
         return logAppender.storeRequest(segment, entry);
      } catch (Exception e) {
         return CompletableFutures.completedExceptionFuture(new PersistenceException(e));
      }
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      try {
         log.tracef("Deleting key %s for segment %d", key, segment);
         return logAppender.deleteRequest(segment, key, marshaller.objectToBuffer(key));
      } catch (Exception e) {
         return CompletableFutures.completedExceptionFuture(new PersistenceException(e));
      }
   }

   @Override
   public CompletionStage<Boolean> containsKey(int segment, Object key) {
      try {
         for (;;) {
            // TODO: consider storing expiration timestamp in temporary table
            EntryPosition entry = temporaryTable.get(segment, key);
            if (entry != null) {
               if (entry.offset < 0) {
                  return CompletableFutures.completedFalse();
               }
               FileProvider.Handle handle = fileProvider.getFile(entry.file);
               if (handle != null) {
                  return blockingManager.supplyBlocking(() -> {
                     try {
                        try {
                           EntryHeader header = EntryRecord.readEntryHeader(handle, entry.offset);
                           if (header == null) {
                              throw new IllegalStateException("Error reading from " + entry.file + ":" + entry.offset + " | " + handle.getFileSize());
                           }
                           return header.expiryTime() < 0 || header.expiryTime() > timeService.wallClockTime();
                        } finally {
                           handle.close();
                        }
                     } catch (IOException e) {
                        throw new CacheException(e);
                     }
                  }, "soft-index-containsKey");
               }
            } else {
               EntryPosition position = index.getPosition(key, segment, marshaller.objectToBuffer(key));
               return CompletableFutures.booleanStage(position != null);
            }
         }
      } catch (Exception e) {
         return CompletableFutures.completedExceptionFuture(log.cannotLoadKeyFromIndex(key, e));
      }
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      return blockingManager.supplyBlocking(() -> {
         log.tracef("Loading key %s for segment %d", key, segment);
         try {
            for (;;) {
               EntryPosition entry = temporaryTable.get(segment, key);
               if (entry != null) {
                  if (entry.offset < 0) {
                     log.tracef("Entry for key=%s found in temporary table on %d:%d but it is a tombstone", key, entry.file, entry.offset);
                     return null;
                  }
                  MarshallableEntry<K, V> marshallableEntry = readValueFromFileOffset(key, entry);
                  if (marshallableEntry != null) {
                     return marshallableEntry;
                  }
               } else {
                  EntryRecord record = index.getRecord(key, segment, marshaller.objectToBuffer(key));
                  if (record == null) {
                     log.tracef("Entry for key=%s not found in index, returning null", key);
                     return null;
                  }
                  return entryFromRecord(record);
               }
            }
         } catch (Exception e) {
            throw log.cannotLoadKeyFromIndex(key, e);
         }
      }, "soft-index-load");
   }

   private MarshallableEntry<K, V> entryFromRecord(EntryRecord record) {
      return marshallableEntryFactory.create(toBuffer(record.getKey()), toBuffer(record.getValue()),
            toBuffer(record.getMetadata()), toBuffer(record.getInternalMetadata()), record.getCreated(), record.getLastUsed());
   }

   private MarshallableEntry<K, V> readValueFromFileOffset(Object key, EntryPosition entry) throws IOException {
      return readValueFromFileOffset(key, entry, false);
   }

   private MarshallableEntry<K, V> readValueFromFileOffset(Object key, EntryPosition entry, boolean includeExpired) throws IOException {
      FileProvider.Handle handle = fileProvider.getFile(entry.file);
      if (handle != null) {
         try {
            EntryHeader header = EntryRecord.readEntryHeader(handle, entry.offset);
            if (header == null) {
               throw new IllegalStateException("Error reading from " + entry.file + ":" + entry.offset + " | " + handle.getFileSize());
            }
            return readEntry(handle, header, entry.offset, key, false,
                  (serializedKey, value, meta, internalMeta, created, lastUsed) ->
                        marshallableEntryFactory.create(serializedKey, value, meta, internalMeta, created, lastUsed),
                  includeExpired);
         } finally {
            handle.close();
         }
      }
      return null;
   }

   private MarshallableEntry<K, V> readEntry(FileProvider.Handle handle, EntryHeader header, int offset,
         Object key, boolean nonNull, EntryCreator<K, V> entryCreator, boolean includeExpired)
         throws IOException {
      if (!includeExpired && header.expiryTime() > 0 && header.expiryTime() <= timeService.wallClockTime()) {
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

   public interface EntryCreator<K,V> {
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

   private <R> Flowable<R> handleFilePublisher(int file, boolean fetchValue, boolean fetchMetadata,
         EntryFunctor<R> functor) {
      return Flowable.using(() -> {
               log.tracef("Loading entries from file %d", file);
               return Optional.ofNullable(fileProvider.getFile(file));
            },
            optHandle -> {
               if (!optHandle.isPresent()) {
                  log.tracef("File %d was deleted during iteration", file);
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
   }

   @Override
   public Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      // TODO: do this more efficiently later
      return Flowable.fromPublisher(publishEntries(segments, filter, false))
            .map(MarshallableEntry::getKey);
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean includeValues) {
      return blockingManager.blockingPublisher(Flowable.defer(() -> {
         Set<Object> seenKeys = new HashSet<>();
         Flowable<Map.Entry<Object, EntryPosition>> tableFlowable = temporaryTable.publish(segments)
               .doOnNext(entry -> seenKeys.add(entry.getKey()));
         if (filter != null) {
            tableFlowable = tableFlowable.filter(entry -> filter.test((K) entry.getKey()));
         }
         Flowable<MarshallableEntry<K, V>> entryFlowable = tableFlowable.flatMapMaybe(entry -> {
            EntryPosition position = entry.getValue();
            if (position.offset < 0) {
               return Maybe.empty();
            }
            MarshallableEntry<K, V> marshallableEntry = readValueFromFileOffset(entry.getKey(), position);
            if (marshallableEntry == null) {
               // Using the key partitioner here isn't the best, however this case should rarely happen
               return Maybe.fromCompletionStage(load(keyPartitioner.getSegment(entry.getKey()), entry.getKey()));
            }
            return Maybe.just(marshallableEntry);
         });
         Flowable<MarshallableEntry<K, V>> indexFlowable = index.publish(segments, includeValues)
               .mapOptional(er -> {
                  if (er.getHeader().valueLength() == 0) {
                     return Optional.empty();
                  }

                  final K key = (K) marshaller.objectFromByteBuffer(er.getKey());
                  if ((filter != null && !filter.test(key)) || seenKeys.contains(key)) {
                     return Optional.empty();
                  }

                  return Optional.of(marshallableEntryFactory.create(key, byteBufferFactory.newByteBuffer(er.getValue()),
                        byteBufferFactory.newByteBuffer(er.getMetadata()),
                        byteBufferFactory.newByteBuffer(er.getInternalMetadata()),
                        er.getCreated(), er.getLastUsed()));
               });

         return Flowable.concat(entryFlowable, indexFlowable);
      }));
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
