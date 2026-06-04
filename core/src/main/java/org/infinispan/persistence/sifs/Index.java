package org.infinispan.persistence.sifs;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.executors.LimitedExecutor;
import org.infinispan.util.SoftBPlusTree;
import org.infinispan.util.SoftBPlusTree.IndexNodeOutdatedException;
import org.infinispan.util.concurrent.NonBlockingManager;

import com.google.errorprone.annotations.concurrent.GuardedBy;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * Keeps the entry positions persisted in a file. It consists of couple of segments, each for one modulo-range of key's
 * hashcodes (according to DataContainer's key equivalence configuration) - writes to each index segment are performed
 * by single thread, having multiple segments spreads the load between them.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class Index {
   private static final Log log = Log.getLog(Index.class);
   private static final int GRACEFULLY = 0x512ACEF3;
   private static final int DIRTY = 0xD112770C;
   // magic(4) + segmentMax(4) + rootOffset(8) + rootOccupied(2) + freeBlocksOffset(8)
   private static final int INDEX_FILE_HEADER_SIZE = 26;

   private final NonBlockingManager nonBlockingManager;
   private final FileProvider dataFileProvider;
   private final FileProvider indexFileProvider;
   private final Path indexDir;
   private final Compactor compactor;
   private final int minNodeSize;
   private final int maxNodeSize;
   // Lock is to protect the flowableProcessors, thus it should be held only to read them
   // and unlock as soon as possible to avoid other locking issues
   private final StampedLock lock = new StampedLock();
   @GuardedBy("lock")
   private final Segment[] segments;
   @GuardedBy("lock")
   private final FlowableProcessor<IndexRequest>[] flowableProcessors;
   private final TimeService timeService;
   private final File indexSizeFile;
   public final AtomicLongArray sizePerSegment;

   private final TemporaryTable temporaryTable;

   private final Executor executor;
   private final double compactionThreshold;

   // This is used to signal that a segment is not currently being used
   private final Segment emptySegment;
   private final FlowableProcessor<IndexRequest> emptyFlowable;

   // This will be set to > 0 if present in the index count
   private long maxSeqId = -1;

   @GuardedBy("lock")
   private CompletionStage<Void> removeSegmentsStage = CompletableFutures.completedNull();

   // Overwrite hooks have been inlined into Segment.accept()

   public Index(NonBlockingManager nonBlockingManager, FileProvider dataFileProvider, Path indexDir, int cacheSegments,
                int minNodeSize, int maxNodeSize, TemporaryTable temporaryTable, Compactor compactor,
                TimeService timeService, Executor executor, int maxOpenFiles,
                double compactionThreshold) throws IOException {
      this.nonBlockingManager = nonBlockingManager;
      this.dataFileProvider = dataFileProvider;
      this.compactor = compactor;
      this.timeService = timeService;
      this.indexDir = indexDir;
      this.minNodeSize = minNodeSize;
      this.maxNodeSize = maxNodeSize;
      this.sizePerSegment = new AtomicLongArray(cacheSegments);
      this.indexFileProvider = new FileProvider(indexDir, maxOpenFiles, "index.", Integer.MAX_VALUE, true);
      this.indexSizeFile = new File(indexDir.toFile(), "index-count");

      this.segments = new Segment[cacheSegments];
      this.flowableProcessors = new FlowableProcessor[cacheSegments];

      this.compactionThreshold = compactionThreshold;
      this.temporaryTable = temporaryTable;
      // Limits the amount of concurrent updates we do to the underlying indices to be based on the number of cache
      // segments. Note that this uses blocking threads so this number is still limited by that as well
      int concurrency = Math.max(cacheSegments >> 4, 2);
      this.executor = new LimitedExecutor("sifs-index", executor, concurrency);
      this.emptySegment = new Segment(this, -1, temporaryTable);
      this.emptySegment.complete(null);

      this.emptyFlowable = UnicastProcessor.<IndexRequest>create()
            .toSerialized();
      emptyFlowable.subscribe(this::handleNonOwnedIndexRequest,
            log::fatalIndexError);
   }

   /**
    * Method used to handle index requests where we do not own the given segment. In this case we just effectively
    * discard the request when possible and update the compactor to have the data as free so it can remove the value
    * later.
    * @param ir request that was sent after we don't own the segment
    */
   private void handleNonOwnedIndexRequest(IndexRequest ir) {
      switch (ir.getType()) {
         case UPDATE:
         case MOVED:
            // We no longer own the segment so just treat the data as free for compactor purposes
            // Note we leave the existing value alone as the removeSegments will take care of that
            compactor.free(ir.getFile(), ir.getSize());
            break;
         case FOUND_OLD:
            throw new IllegalStateException("This is only possible when building the index");
         case SYNC_REQUEST:
            Runnable runnable = (Runnable) ir.getKey();
            runnable.run();
            break;
         case CLEAR:
         case DROPPED:
            // Both drop and clear do nothing as we are already removing the index
      }
      ir.complete(null);
   }

   private boolean checkForExistingIndexSizeFile() {
      int cacheSegments = sizePerSegment.length();
      boolean validCount = false;
      try (RandomAccessFile indexCount = new RandomAccessFile(indexSizeFile, "r")) {
         int cacheSegmentsCount = UnsignedNumeric.readUnsignedInt(indexCount);
         if (cacheSegmentsCount == cacheSegments) {
            for (int i = 0; i < sizePerSegment.length(); ++i) {
               long value = UnsignedNumeric.readUnsignedLong(indexCount);
               if (value < 0) {
                  log.tracef("Found an invalid size for a segment, assuming index is a different format");
                  return false;
               }
               sizePerSegment.set(i, value);
            }

            try {
               maxSeqId = UnsignedNumeric.readUnsignedLong(indexCount);
            } catch (EOFException e) {
               log.tracef("Index didn't contain sequence id, will need to calculate, still retaining index");
            }
            validCount = true;
         } else {
            log.tracef("Previous index file cache segments %d doesn't match configured cache segments %d", cacheSegmentsCount, cacheSegments);
         }
      } catch (IOException e) {
         log.tracef("Encountered IOException %s while reading index count file, assuming index dirty", e.getMessage());
      }

      // Delete this so the file doesn't exist and will be written at stop. If the file isn't present it is considered dirty
      indexSizeFile.delete();
      return validCount;
   }

   public static byte[] toIndexKey(org.infinispan.commons.io.ByteBuffer buffer) {
      return toIndexKey(buffer.getBuf(), buffer.getOffset(), buffer.getLength());
   }

   static byte[] toIndexKey(byte[] bytes, int offset, int length) {
      if (offset == 0 && length == bytes.length) {
         return bytes;
      }
      byte[] indexKey = new byte[length];
      System.arraycopy(bytes, 0, indexKey, 0, length);

      return indexKey;
   }

   /**
    * @return True if the index was loaded from well persisted state
    */
   public boolean load() {
      boolean loaded = attemptLoad();

      // If we failed to load any of the index we have to make sure to clear anything we may have loaded
      if (!loaded) {
         maxSeqId = -1;
         compactor.getFileStats().clear();
         for (int i = 0; i < sizePerSegment.length(); ++i) {
            sizePerSegment.set(i, 0);
         }
      }
      return loaded;
   }

   private boolean attemptLoad() {
      if (!checkForExistingIndexSizeFile()) {
         return false;
      }
      try {
         File statsFile = new File(indexDir.toFile(), "index.stats");
         if (!statsFile.exists()) {
            return false;
         }
         try (FileChannel statsChannel = new RandomAccessFile(statsFile, "rw").getChannel()) {

            // id / length / free / expirationTime
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + 8);
            while (read(statsChannel, buffer)) {
               buffer.flip();
               int id = buffer.getInt();
               int length = buffer.getInt();
               int free = buffer.getInt();
               long expirationTime = buffer.getLong();
               if (!compactor.addFreeFile(id, length, free, expirationTime, false)) {
                  log.tracef("Unable to add free file: %s ", id);
                  return false;
               }
               log.tracef("Loading file info for file: %s with total: %s, free: %s", id, length, free);
               buffer.flip();
            }
         }

         // No reason to keep around after loading
         statsFile.delete();

         for (Segment segment : segments) {
            if (!segment.load()) return false;
         }
         return true;
      } catch (IOException e) {
         log.trace("Exception encountered while attempting to load index, assuming index is bad", e);
         return false;
      }
   }

   public void reset() throws IOException {
      for (Segment segment : segments) {
         segment.reset();
      }
   }

   public EntryRecord getRecord(Object key, int cacheSegment, org.infinispan.commons.io.ByteBuffer serializedKey) throws IOException {
      return getRecord(cacheSegment, toIndexKey(serializedKey), true);
   }

   public EntryRecord getRecordEvenIfExpired(Object key, int cacheSegment, byte[] serializedKey) throws IOException {
      return getRecord(cacheSegment, serializedKey, false);
   }

   private EntryRecord getRecord(int cacheSegment, byte[] indexKey, boolean checkExpiration) throws IOException {
      long stamp = lock.readLock();
      try {
         IndexEntry entry = segments[cacheSegment].tree.get(indexKey);
         if (entry == null) {
            log.tracef("No entry found in index for segment %d", cacheSegment);
            return null;
         }
         long wallClockTime = checkExpiration ? timeService.wallClockTime() : -1;
         return entry.loadRecord(dataFileProvider, wallClockTime, true, true);
      } finally {
         lock.unlockRead(stamp);
      }
   }

   public EntryPosition getPosition(Object key, int cacheSegment, org.infinispan.commons.io.ByteBuffer serializedKey) throws IOException {
      long stamp = lock.readLock();
      try {
         byte[] indexKey = toIndexKey(serializedKey);
         IndexEntry entry = segments[cacheSegment].tree.get(indexKey);
         if (entry == null) {
            log.tracef("No position found in index for key %s segment %d", key, cacheSegment);
            return null;
         }
         EntryHeader header = entry.getHeader(dataFileProvider);
         if (header == null) return null;
         if (header.expiryTime() > 0 && header.expiryTime() <= timeService.wallClockTime()) {
            if (log.isTraceEnabled()) {
               log.tracef("Found node on %d:%d but it is expired", entry.file, entry.offset);
            }
            return null;
         }
         return entry;
      } finally {
         lock.unlockRead(stamp);
      }
   }

   public EntryInfo getInfo(Object key, int cacheSegment, byte[] serializedKey) throws IOException {
      long stamp = lock.readLock();
      try {
         return segments[cacheSegment].tree.get(serializedKey);
      } finally {
         lock.unlockRead(stamp);
      }
   }

   public CompletionStage<Void> clear() {
      log.tracef("Clearing index");
      long stamp;
      if ((stamp = lock.tryWriteLock()) != 0) {
         // actualSubmitClear handles all the lock release calls
         return actualSubmitClear(stamp);
      } else {
         return CompletableFuture.supplyAsync(() -> {
            long innerStamp = lock.writeLock();
            return actualSubmitClear(innerStamp);
         }, executor).thenCompose(Function.identity());
      }
   }

   private CompletionStage<Void> actualSubmitClear(long writeStampToUnlock) {
      // Collect clear requests while holding the lock, then release immediately
      AggregateCompletionStage<Void> stage = CompletionStages.aggregateCompletionStage();
      try {
         for (FlowableProcessor<IndexRequest> processor : flowableProcessors) {
            // Ignore emptyFlowable as we use this to signal that we don't own that segment anymore
            if (processor == emptyFlowable) {
               continue;
            }
            IndexRequest clearRequest = IndexRequest.clearRequest();
            processor.onNext(clearRequest);
            stage.dependsOn(clearRequest);
         }
      } catch (Throwable t) {
         lock.unlockWrite(writeStampToUnlock);
         log.debugf(t, "Clear encountered exception while submitting requests");
         throw t;
      } finally {
         lock.unlockWrite(writeStampToUnlock);
      }

      // Now wait for clear requests to complete without holding the lock
      return stage.freeze().whenComplete((ignore, t) -> {
         if (t != null) {
            log.clearError(t);
         } else {
            log.tracef("Clear has completed");
         }
      });
   }

   public CompletionStage<Object> handleRequest(IndexRequest indexRequest) {
      flowableProcessors[indexRequest.getSegment()].onNext(indexRequest);
      return indexRequest;
   }

   public CompletionStage<Void> ensureRunOnLast(Runnable runnable) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      AtomicInteger count = new AtomicInteger(flowableProcessors.length);
      IndexRequest request = IndexRequest.syncRequest(() -> {
         if (count.decrementAndGet() == 0) {
            try {
               runnable.run();
               nonBlockingManager.complete(future, null);
            } catch (Throwable t) {
               nonBlockingManager.completeExceptionally(future, t);
            }
         }
      });
      for (FlowableProcessor<IndexRequest> flowableProcessor : flowableProcessors) {
         flowableProcessor.onNext(request);
      }
      return future;
   }

   public CompletionStage<Void> deleteFileAsync(int fileId) {
      return ensureRunOnLast(() -> {
         // After all indexes have ensured they have processed all requests - the last one will delete the file
         // This guarantees that the index can't see an outdated value
         dataFileProvider.deleteFile(fileId);
         compactor.releaseStats(fileId);
      });
   }

   public CompletionStage<Void> stop(long maxSeqId) throws InterruptedException {
      AggregateCompletionStage<Void> aggregateCompletionStage;
      long stamp = lock.readLock();
      try {
         for (FlowableProcessor<IndexRequest> flowableProcessor : flowableProcessors) {
            flowableProcessor.onComplete();
         }

         aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
         for (Segment segment : segments) {
            aggregateCompletionStage.dependsOn(segment);
         }

         aggregateCompletionStage.dependsOn(removeSegmentsStage);
      } finally {
         lock.unlockRead(stamp);
      }

      // After all SIFS segments are complete we write the size
      return aggregateCompletionStage.freeze().thenRun(() -> {
         indexFileProvider.stop();
         try {
            // Create the file first as it should not be present as we deleted during startup
            indexSizeFile.createNewFile();
            try (FileOutputStream indexCountStream = new FileOutputStream(indexSizeFile)) {
               UnsignedNumeric.writeUnsignedInt(indexCountStream, this.sizePerSegment.length());
               for (int i = 0; i < sizePerSegment.length(); ++i) {
                  UnsignedNumeric.writeUnsignedLong(indexCountStream, sizePerSegment.get(i));
               }
               UnsignedNumeric.writeUnsignedLong(indexCountStream, maxSeqId);
            }

            ConcurrentMap<Integer, Compactor.Stats> map = compactor.getFileStats();
            File statsFile = new File(indexDir.toFile(), "index.stats");

            try (FileChannel statsChannel = new RandomAccessFile(statsFile, "rw").getChannel()) {
               statsChannel.truncate(0);
               // Maximum size that all ints and long can add up to
               ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + 8);
               for (Map.Entry<Integer, Compactor.Stats> entry : map.entrySet()) {
                  int file = entry.getKey();
                  int total = entry.getValue().getTotal();
                  if (total == -1) {
                     total = (int) dataFileProvider.getFileSize(file);
                  }
                  int free = entry.getValue().getFree();
                  buffer.putInt(file);
                  buffer.putInt(total);
                  buffer.putInt(free);
                  buffer.putLong(entry.getValue().getNextExpirationTime());
                  buffer.flip();
                  write(statsChannel, buffer);
                  buffer.flip();
               }

               // Force the stats written since some tests may read it after shutting down
               statsChannel.force(false);
            }
         } catch (IOException e) {
            throw CompletableFutures.asCompletionException(e);
         }
      });
   }

   public long approximateSize(IntSet cacheSegments) {
      long size = 0;
      for (PrimitiveIterator.OfInt segIter = cacheSegments.iterator(); segIter.hasNext(); ) {
         int cacheSegment = segIter.nextInt();
         size += sizePerSegment.get(cacheSegment);
         if (size < 0) {
            return Long.MAX_VALUE;
         }
      }

      return size;
   }

   /**
    * Returns the maximum sequence id or -1 if it is not yet set. This method does not calculate the sequence. If you
    * want to retrieve an initialized sequence id you can use {@link #getOrCalculateMaxSeqId()} which will calculate
    * it if needed and return it.
    * @return the maximum sequence id or -1 if not yet calculated
    */
   public long getMaxSeqId() {
      return maxSeqId;
   }

   /**
    * Returns the maximum sequence id for the index which will always be 0 or greater
    * @return the maximum sequence id to use for writes
    */
   public long getOrCalculateMaxSeqId() throws IOException {
      if (maxSeqId > 0) {
         return maxSeqId;
      }
      ByRef.Long maxSeq = new ByRef.Long(0);
      long stamp = lock.readLock();
      try {
         for (Segment seg : segments) {
            CompletionStages.join(seg.tree.<Void>publish((key, info) -> {
               try (FileProvider.Handle handle = dataFileProvider.getFile(info.file)) {
                  if (handle != null) {
                     int readOffset = info.offset < 0 ? ~info.offset : info.offset;
                     EntryHeader header = EntryRecord.readEntryHeader(handle, readOffset);
                     if (header != null) {
                        maxSeq.set(Math.max(maxSeq.get(), header.seqId()));
                     }
                  }
               }
               return null;
            }, true).ignoreElements().toCompletionStage(null));
         }
      } finally {
         lock.unlockRead(stamp);
      }
      return maxSeq.get();
   }

   public void start(IntSet segments) {
      addSegments(segments);
   }

   static boolean read(FileProvider.Handle handle, ByteBuffer buffer, long offset) throws IOException {
      assert buffer.hasRemaining();
      int read = 0;
      do {
         int newRead = handle.read(buffer, offset + read);
         if (newRead < 0) {
            return false;
         }
         read += newRead;
      } while (buffer.hasRemaining());
      return true;
   }

   static boolean read(FileChannel channel, ByteBuffer buffer) throws IOException {
      assert buffer.hasRemaining();
      do {
         int read = channel.read(buffer);
         if (read < 0) {
            return false;
         }
      } while (buffer.position() < buffer.limit());
      return true;
   }

   private static long write(FileProvider.Handle handle, ByteBuffer buffer, long offset) throws IOException {
      assert buffer.hasRemaining();
      long write = 0;
      while (buffer.hasRemaining()) {
         write += handle.write(buffer, offset + write);
      }
      return write;
   }

   private static void write(FileChannel indexFile, ByteBuffer buffer) throws IOException {
      assert buffer.hasRemaining();
      do {
         int written = indexFile.write(buffer);
         if (written < 0) {
            throw new IllegalStateException("Cannot write to index file!");
         }
      } while (buffer.position() < buffer.limit());
   }

   public CompletionStage<Void> addSegments(IntSet addedSegments) {
      long stamp;
      // Since actualAddSegments doesn't block we try a quick write lock acquisition to possibly avoid context change
      if ((stamp = lock.tryWriteLock()) != 0) {
         try {
            actualAddSegments(addedSegments);
         } finally {
            lock.unlockWrite(stamp);
         }
         return CompletableFutures.completedNull();
      }
      return CompletableFuture.runAsync(() -> {
         long innerStamp = lock.writeLock();
         try {
            actualAddSegments(addedSegments);
         } finally {
            lock.unlockWrite(innerStamp);
         }
      }, executor);
   }

   private void traceSegmentsAdded(IntSet addedSegments) {
      IntSet actualAddedSegments = IntSets.mutableEmptySet(segments.length);
      for (PrimitiveIterator.OfInt segmentIter = addedSegments.iterator(); segmentIter.hasNext(); ) {
         int i = segmentIter.nextInt();
         if (segments[i] == null || segments[i] == emptySegment) {
            actualAddedSegments.add(i);
         }
      }
      log.tracef("Adding segments %s to SIFS index", actualAddedSegments);
   }

   private void actualAddSegments(IntSet addedSegments) {
      if (log.isTraceEnabled()) {
         traceSegmentsAdded(addedSegments);
      }
      for (PrimitiveIterator.OfInt segmentIter = addedSegments.iterator(); segmentIter.hasNext(); ) {
         int i = segmentIter.nextInt();

         // Segment is already running, don't do anything
         if (segments[i] != null && segments[i] != emptySegment) {
            continue;
         }

         UnicastProcessor<IndexRequest> flowableProcessor = UnicastProcessor.create(false);
         Segment segment = new Segment(this, i, temporaryTable);

         // Note we do not load the segments here as we only have to load them on startup and not if segments are
         // added at runtime
         this.segments[i] = segment;
         // It is possible to write from multiple threads
         this.flowableProcessors[i] = flowableProcessor.toSerialized();

         flowableProcessors[i]
               .observeOn(Schedulers.from(executor))
               .subscribe(segment, t -> {
                  log.error("Error encountered with index, SIFS may not operate properly.", t);
                  segment.completeExceptionally(t);
               }, segment);
      }
   }
   public CompletionStage<Void> removeSegments(IntSet removedCacheSegments) {
      long stamp;
      // Use a try lock to avoid context switch if possible
      if ((stamp = lock.tryWriteLock()) != 0) {
         try {
            // This method doesn't block if we can acquire lock immediately, just replaces segments and flowables
            // and submits an async task
            actualRemoveSegments(removedCacheSegments);
         } finally {
            lock.unlockWrite(stamp);
         }
         return CompletableFutures.completedNull();
      }
      return CompletableFuture.runAsync(() -> {
         long innerStamp = lock.writeLock();
         try {
            actualRemoveSegments(removedCacheSegments);
         } finally {
            lock.unlockWrite(innerStamp);
         }
      }, executor);
   }

   private void actualRemoveSegments(IntSet removedCacheSegments) {
      log.tracef("Removing segments %s from index", removedCacheSegments);
      int addedCount = removedCacheSegments.size();
      List<Segment> removedSegments = new ArrayList<>(addedCount);
      List<FlowableProcessor<IndexRequest>> removedFlowables = new ArrayList<>(addedCount);
      CompletableFuture<Void> stageWhenComplete = new CompletableFuture<>();

      for (PrimitiveIterator.OfInt iter = removedCacheSegments.iterator(); iter.hasNext(); ) {
         int i = iter.nextInt();
         // If the segment was not owned by us don't do anything with it
         if (segments[i] != emptySegment) {
            removedSegments.add(segments[i]);
            segments[i] = emptySegment;
            removedFlowables.add(flowableProcessors[i]);
            flowableProcessors[i] = emptyFlowable;

            sizePerSegment.set(i, 0);
         }
      }
      // Technically this is an issue with sequential removeSegments being called and then shutdown, but
      // we don't support data consistency with non-shared stores in a cluster (this method is only called in a cluster).
      removeSegmentsStage = stageWhenComplete;

      executor.execute(() -> {
         try {
            log.tracef("Cleaning old index information for segments: %s", removedCacheSegments);
            // We would love to do this outside of this stage asynchronously but unfortunately we can't say we have
            // removed the segments until the segment file is deleted
            AggregateCompletionStage<Void> stage = CompletionStages.aggregateCompletionStage();
            // Then we need to delete the segment
            for (int offset = 0; offset < removedSegments.size(); ++offset) {

               // We signal to complete the flowables, once complete the index is updated as we need to
               // update the compactor
               removedFlowables.get(offset).onComplete();
               Segment segment = removedSegments.get(offset);
               stage.dependsOn(
                     segment.thenCompose(v ->
                                 segment.tree.<Void>publish((key, info) -> {
                                    try (FileProvider.Handle handle = dataFileProvider.getFile(info.file)) {
                                       if (handle != null) {
                                          int readOffset = info.offset < 0 ? ~info.offset : info.offset;
                                          EntryHeader header = EntryRecord.readEntryHeader(handle, readOffset);
                                          if (header != null) {
                                             compactor.freeIfPresent(info.file, header.totalLength());
                                          }
                                       }
                                    }
                                    return null;
                                 }, true).ignoreElements().toCompletionStage(null))
                           .thenRun(segment::delete));
            }
            stage.freeze()
                  .whenComplete((___, t) -> {
                     if (t != null) {
                        stageWhenComplete.completeExceptionally(t);
                     } else {
                        stageWhenComplete.complete(null);
                     }
                  });
         } catch (Throwable t) {
            stageWhenComplete.completeExceptionally(t);
         }
      });
   }

   static final SoftBPlusTree.ValueSerializer<IndexEntry> INDEX_ENTRY_SERIALIZER =
         new SoftBPlusTree.ValueSerializer<>() {
            @Override
            public void write(IndexEntry value, ByteBuffer buffer) {
               buffer.putInt(value.file);
               buffer.putInt(value.offset);
               buffer.putInt(value.numRecords);
            }

            @Override
            public IndexEntry read(ByteBuffer buffer) {
               return new IndexEntry(buffer.getInt(), buffer.getInt(), buffer.getInt());
            }

            @Override
            public int serializedSize(IndexEntry value) {
               return 12;
            }
         };

   /**
    * Block-allocated node store backed by a file. Reuses freed blocks to
    * avoid unbounded index file growth.
    */
   static class IndexFileNodeStore implements SoftBPlusTree.NodeStore {
      private final FileProvider indexFileProvider;
      private final int fileId;

      IndexFileNodeStore(FileProvider indexFileProvider, int fileId) {
         this.indexFileProvider = indexFileProvider;
         this.fileId = fileId;
      }

      @Override
      public void write(ByteBuffer data, long offset) throws IOException {
         try (FileProvider.Handle handle = indexFileProvider.getFile(fileId)) {
            long pos = offset;
            while (data.hasRemaining()) {
               pos += handle.write(data, pos);
            }
         }
      }

      @Override
      public ByteBuffer read(long offset, int length) throws IOException {
         ByteBuffer buffer = ByteBuffer.allocate(length);
         try (FileProvider.Handle handle = indexFileProvider.getFile(fileId)) {
            if (handle == null) {
               throw new IndexNodeOutdatedException(fileId + ":" + offset);
            }
            int totalRead = 0;
            while (totalRead < length) {
               int n = handle.read(buffer, offset + totalRead);
               if (n < 0) {
                  throw new IOException("Truncated node store at offset " + offset);
               }
               totalRead += n;
            }
         }
         buffer.flip();
         return buffer;
      }

      @Override
      public void truncate(long size) throws IOException {
         try (FileProvider.Handle handle = indexFileProvider.getFile(fileId)) {
            handle.truncate(size);
         }
      }
   }

   static class Segment extends CompletableFuture<Void> implements Consumer<IndexRequest>, Action {
      private static final short BLOCK_ALIGNMENT = 64;

      final Index index;
      private final TemporaryTable temporaryTable;
      private final int id;
      private final IndexFileNodeStore nodeStore;
      private final SoftBPlusTree.KeyLoader<IndexEntry> keyLoader;

      volatile SoftBPlusTree<IndexEntry> tree;

      private Segment(Index index, int id, TemporaryTable temporaryTable) {
         this.index = index;
         this.temporaryTable = temporaryTable;
         this.id = id;
         this.nodeStore = new IndexFileNodeStore(index.indexFileProvider, id);
         this.keyLoader = value -> (value).loadKey(index.dataFileProvider);
         this.tree = new SoftBPlusTree<>(index.minNodeSize, index.maxNodeSize, nodeStore,
               INDEX_ENTRY_SERIALIZER, keyLoader, BLOCK_ALIGNMENT, INDEX_FILE_HEADER_SIZE);
      }

      public int getId() {
         return id;
      }

      boolean load() throws IOException {
         int segmentMax = temporaryTable.getSegmentMax();
         FileProvider.Handle handle = index.indexFileProvider.getFile(id);
         try (handle) {
            if (handle.getFileSize() < INDEX_FILE_HEADER_SIZE) {
               return handle.getFileSize() == 0 && index.sizePerSegment.get(id) == 0;
            }
            ByteBuffer header = ByteBuffer.allocate(INDEX_FILE_HEADER_SIZE);
            if (!read(handle, header, 0)) {
               return handle.getFileSize() == 0 && index.sizePerSegment.get(id) == 0;
            }
            if (header.getInt(0) != GRACEFULLY || header.getInt(4) != segmentMax) {
               handle.truncate(0);
               tree = new SoftBPlusTree<>(index.minNodeSize, index.maxNodeSize, nodeStore,
                     INDEX_ENTRY_SERIALIZER, keyLoader, BLOCK_ALIGNMENT, INDEX_FILE_HEADER_SIZE);
               return false;
            }
            long rootOffset = header.getLong(8);
            short rootOccupiedSpace = header.getShort(16);
            long freeBlocksOffset = header.getLong(18);
            // Mark as dirty
            ByteBuffer dirty = ByteBuffer.allocate(4);
            dirty.putInt(0, DIRTY);
            write(handle, dirty, 0);
            if (rootOffset == 0) {
               return true;
            }
            SoftBPlusTree<IndexEntry> softTree = new SoftBPlusTree<>(index.minNodeSize, index.maxNodeSize,
                  nodeStore, INDEX_ENTRY_SERIALIZER, keyLoader, BLOCK_ALIGNMENT, INDEX_FILE_HEADER_SIZE);
            softTree.setStoreSize(freeBlocksOffset);
            // Restore free-block state from freeBlocksOffset
            int freeBlocksLen = (int) (handle.getFileSize() - freeBlocksOffset);
            if (freeBlocksLen > 0) {
               ByteBuffer freeBlocksBuf = ByteBuffer.allocate(freeBlocksLen);
               read(handle, freeBlocksBuf, freeBlocksOffset);
               freeBlocksBuf.flip();
               softTree.deserializeFreeBlocks(freeBlocksBuf);
            }
            SoftBPlusTree.NodeSpace rootSpace = new SoftBPlusTree.NodeSpace(rootOffset, rootOccupiedSpace);
            softTree.loadTree(rootSpace);
            tree = softTree;
            return true;
         }
      }

      void delete() {
         if (id >= 0) {
            log.tracef("Deleting file for index %s", id);
            index.indexFileProvider.deleteFile(id);
         }
      }

      void reset() throws IOException {
         tree.clear();
      }

      @Override
      public void accept(IndexRequest request) throws Throwable {
         if (log.isTraceEnabled()) log.tracef("Indexing %s", request);
         int cacheSegment = request.getSegment();
         switch (request.getType()) {
            case CLEAR:
               tree.clear();
               index.sizePerSegment.set(id, 0);
               index.nonBlockingManager.complete(request, null);
               return;
            case SYNC_REQUEST:
               Runnable runnable = (Runnable) request.getKey();
               runnable.run();
               index.nonBlockingManager.complete(request, null);
               return;
            case UPDATE:
               handleUpdate(request, cacheSegment);
               break;
            case MOVED:
               handleMoved(request, cacheSegment);
               break;
            case DROPPED:
               handleDropped(request, cacheSegment);
               break;
            case FOUND_OLD:
               handleFoundOld(request, cacheSegment);
               break;
            default:
               throw new IllegalArgumentException(request.toString());
         }
         temporaryTable.removeConditionally(request.getSegment(), request.getKey(), request.getFile(), request.getOffset());
         if (request.getType() != IndexRequest.Type.UPDATE) {
            index.nonBlockingManager.complete(request, null);
         }
      }

      private void handleUpdate(IndexRequest request, int cacheSegment) throws IOException {
         byte[] indexKey = Index.toIndexKey(request.getSerializedKey());
         int file = request.getFile();
         int offset = request.getOffset();

         IndexEntry existing = tree.get(indexKey);
         if (existing != null) {
            int numRecords = existing.numRecords + 1;
            int oldTotalLength = loadTotalLength(existing);
            index.compactor.free(existing.file, oldTotalLength);
            tree.put(indexKey, new IndexEntry(file, offset, numRecords));
            index.nonBlockingManager.complete(request, true);
            if (offset >= 0 && existing.offset < 0) {
               index.sizePerSegment.incrementAndGet(cacheSegment);
            } else if (offset < 0 && existing.offset >= 0) {
               index.sizePerSegment.decrementAndGet(cacheSegment);
            }
         } else {
            tree.put(indexKey, new IndexEntry(file, offset, 1));
            index.nonBlockingManager.complete(request, false);
            if (offset >= 0) {
               index.sizePerSegment.incrementAndGet(cacheSegment);
            }
         }
      }

      private void handleMoved(IndexRequest request, int cacheSegment) throws IOException {
         byte[] indexKey = Index.toIndexKey(request.getSerializedKey());
         int file = request.getFile();
         int offset = request.getOffset();
         int size = request.getSize();

         IndexEntry existing = tree.get(indexKey);
         if (existing != null
               && existing.file == request.getPrevFile()
               && existing.offset == request.getPrevOffset()) {
            int oldTotalLength = loadTotalLength(existing);
            index.compactor.free(existing.file, oldTotalLength);
            tree.put(indexKey, new IndexEntry(file, offset, existing.numRecords));
            if (offset < 0 && request.getPrevOffset() >= 0) {
               index.sizePerSegment.decrementAndGet(cacheSegment);
            }
         } else {
            index.compactor.free(file, size);
         }
      }

      private void handleDropped(IndexRequest request, int cacheSegment) throws IOException {
         byte[] indexKey = Index.toIndexKey(request.getSerializedKey());

         IndexEntry existing = tree.get(indexKey);
         if (existing == null) return;

         boolean canDecrease = existing.numRecords > 1 || existing.file < 0
               || (existing.file == request.getPrevFile()
                     && (existing.offset == request.getPrevOffset()
                           || existing.offset == ~request.getPrevOffset()));
         if (!canDecrease) {
            log.tracef("DECREASE ignored: numRecords: %d, existing %s, request %s",
                  existing.numRecords, existing, request);
            return;
         }

         int numRecords = existing.numRecords - 1;
         if (numRecords > 0) {
            tree.put(indexKey, new IndexEntry(existing.file, existing.offset, numRecords));
         } else {
            tree.remove(indexKey);
            int oldTotalLength = loadTotalLength(existing);
            index.compactor.free(existing.file, oldTotalLength);
         }
         if (request.getPrevFile() == existing.file && request.getPrevOffset() == existing.offset) {
            index.sizePerSegment.decrementAndGet(cacheSegment);
         }
      }

      private void handleFoundOld(IndexRequest request, int cacheSegment) throws IOException {
         byte[] indexKey = Index.toIndexKey(request.getSerializedKey());
         int file = request.getFile();
         int size = request.getSize();

         IndexEntry existing = tree.get(indexKey);
         if (existing != null) {
            tree.put(indexKey, new IndexEntry(existing.file, existing.offset, existing.numRecords + 1));
            index.compactor.free(file, size);
         } else {
            tree.put(indexKey, new IndexEntry(file, request.getOffset(), 1));
         }
      }

      private int loadTotalLength(IndexEntry entry) throws IOException {
         EntryHeader header = entry.getHeader(index.dataFileProvider);
         return header != null ? header.totalLength() : 0;
      }

      @Override
      public void run() throws IOException {
         try {
            SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
            try (FileProvider.Handle handle = index.indexFileProvider.getFile(id)) {
               // Write free blocks at current end of node data
               ByteBuffer freeBlocksBuf = tree.serializeFreeBlocks();
               long freeBlocksOffset = tree.getStoreSize();
               write(handle, freeBlocksBuf, freeBlocksOffset);

               // Write header bytes 4-25 (segmentMax, rootOffset, rootOccupied, freeBlocksOffset)
               int headerWithoutMagic = INDEX_FILE_HEADER_SIZE - 4;
               ByteBuffer header = ByteBuffer.allocate(headerWithoutMagic);
               header.putInt(index.segments.length);
               header.putLong(rootSpace != null ? rootSpace.offset() : 0);
               header.putShort(rootSpace != null ? rootSpace.occupiedSpace() : 0);
               header.putLong(freeBlocksOffset);
               header.flip();
               write(handle, header, 4);

               // Write magic last so a crash leaves the file marked dirty
               header.clear();
               header.limit(4);
               header.putInt(0, GRACEFULLY);
               write(handle, header, 0);
            }

            complete(null);
         } catch (Throwable t) {
            completeExceptionally(t);
         }
      }

      public FileProvider.Handle getIndexFile() throws IOException {
         return index.indexFileProvider.getFile(id);
      }

      public FileProvider getFileProvider() {
         return index.dataFileProvider;
      }

      public Compactor getCompactor() {
         return index.compactor;
      }

      public TimeService getTimeService() {
         return index.timeService;
      }
   }

   Flowable<EntryRecord> publish(IntSet cacheSegments, boolean loadValues) {
      return Flowable.fromIterable(cacheSegments)
            .concatMap(cacheSegment -> publish(cacheSegment, loadValues));
   }

   Flowable<EntryRecord> publish(int cacheSegment, boolean loadValues) {
      long stamp = lock.readLock();
      try {
         var segment = segments[cacheSegment];
         if (sizePerSegment.get(cacheSegment) == 0) {
            lock.unlockRead(stamp);
            return Flowable.empty();
         }
         SoftBPlusTree<IndexEntry> tree = segment.tree;

         long wallClockTime = timeService.wallClockTime();
         return tree.publish((key, entry) ->
                     entry.loadRecord(dataFileProvider, wallClockTime, loadValues, false), false)
               .doFinally(() -> lock.unlockRead(stamp));
      } catch (Throwable t) {
         lock.unlockRead(stamp);
         throw t;
      }
   }
}
