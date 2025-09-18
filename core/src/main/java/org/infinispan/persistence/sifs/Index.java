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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.executors.LimitedExecutor;
import org.infinispan.util.concurrent.NonBlockingManager;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import io.reactivex.rxjava3.schedulers.Schedulers;
import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * Keeps the entry positions persisted in a file. It consists of couple of segments, each for one modulo-range of key's
 * hashcodes (according to DataContainer's key equivalence configuration) - writes to each index segment are performed
 * by single thread, having multiple segments spreads the load between them.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class Index {
   private static final Log log = LogFactory.getLog(Index.class, Log.class);
   // PRE ISPN 14.0.22 GRACEFULLY VALUE = 0x512ACEF1;
   private static final int GRACEFULLY = 0x512ACEF2;
   private static final int DIRTY = 0xD112770C;
   // 4 bytes for graceful shutdown
   // 4 bytes for segment max (this way the index can be regenerated if number of segments change
   // 8 bytes root offset
   // 2 bytes root occupied
   // 8 bytes free block offset
   // 8 bytes number of elements
   private static final int INDEX_FILE_HEADER_SIZE = 34;

   private final NonBlockingManager nonBlockingManager;
   private final FileProvider dataFileProvider;
   private final FileProvider indexFileProvider;
   private final Path indexDir;
   private final Compactor compactor;
   private final int minNodeSize;
   private final int maxNodeSize;
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

   // This is used to signal that a segment is not currently being used
   private final Segment emptySegment;
   private final FlowableProcessor<IndexRequest> emptyFlowable;

   // This will be set to > 0 if present in the index count
   private long maxSeqId = -1;

   @GuardedBy("lock")
   private CompletionStage<Void> removeSegmentsStage = CompletableFutures.completedNull();

   private final IndexNode.OverwriteHook movedHook = new IndexNode.OverwriteHook() {
      @Override
      public boolean check(IndexRequest request, int oldFile, int oldOffset) {
         return oldFile == request.getPrevFile() && oldOffset == request.getPrevOffset();
      }

      @Override
      public void setOverwritten(IndexRequest request, int cacheSegment, boolean overwritten, int prevFile, int prevOffset) {
         if (overwritten && request.getOffset() < 0 && request.getPrevOffset() >= 0) {
            sizePerSegment.decrementAndGet(cacheSegment);
         }
      }
   };

   private final IndexNode.OverwriteHook updateHook = new IndexNode.OverwriteHook() {
      @Override
      public void setOverwritten(IndexRequest request, int cacheSegment, boolean overwritten, int prevFile, int prevOffset) {
         nonBlockingManager.complete(request, overwritten);
         if (request.getOffset() >= 0 && prevOffset < 0) {
            sizePerSegment.incrementAndGet(cacheSegment);
         } else if (request.getOffset() < 0 && prevOffset >= 0) {
            sizePerSegment.decrementAndGet(cacheSegment);
         }
      }
   };

   private final IndexNode.OverwriteHook droppedHook = new IndexNode.OverwriteHook() {
      @Override
      public void setOverwritten(IndexRequest request, int cacheSegment, boolean overwritten, int prevFile, int prevOffset) {
         if (request.getPrevFile() == prevFile && request.getPrevOffset() == prevOffset) {
            sizePerSegment.decrementAndGet(cacheSegment);
         }
      }
   };

   public Index(NonBlockingManager nonBlockingManager, FileProvider dataFileProvider, Path indexDir, int cacheSegments,
                int minNodeSize, int maxNodeSize, TemporaryTable temporaryTable, Compactor compactor,
                TimeService timeService, Executor executor, int maxOpenFiles) throws IOException {
      this.nonBlockingManager = nonBlockingManager;
      this.dataFileProvider = dataFileProvider;
      this.compactor = compactor;
      this.timeService = timeService;
      this.indexDir = indexDir;
      this.minNodeSize = minNodeSize;
      this.maxNodeSize = maxNodeSize;
      this.sizePerSegment = new AtomicLongArray(cacheSegments);
      this.indexFileProvider = new FileProvider(indexDir, maxOpenFiles, "index.", Integer.MAX_VALUE);
      this.indexSizeFile = new File(indexDir.toFile(), "index-count");

      this.segments = new Segment[cacheSegments];
      this.flowableProcessors = new FlowableProcessor[cacheSegments];

      this.temporaryTable = temporaryTable;
      // Limits the amount of concurrent updates we do to the underlying indices to be based on the number of cache
      // segments. Note that this uses blocking threads so this number is still limited by that as well
      int concurrency = Math.max(cacheSegments >> 4, 1);
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
            log.tracef("Previous index file cache segments " + cacheSegmentsCount + " doesn't match configured" +
                  " cache segments " + cacheSegments);
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

   /**
    * Get record or null if expired
    */
   public EntryRecord getRecord(Object key, int cacheSegment, org.infinispan.commons.io.ByteBuffer serializedKey) throws IOException {
      return getRecord(key, cacheSegment, toIndexKey(serializedKey), IndexNode.ReadOperation.GET_RECORD);
   }

   /**
    * Get record (even if expired) or null if not present
    */
   public EntryRecord getRecordEvenIfExpired(Object key, int cacheSegment, byte[] serializedKey) throws IOException {
      return getRecord(key, cacheSegment, serializedKey, IndexNode.ReadOperation.GET_EXPIRED_RECORD);
   }

   private EntryRecord getRecord(Object key, int cacheSegment, byte[] indexKey, IndexNode.ReadOperation readOperation) throws IOException {
      long stamp = lock.readLock();
      try {
         return IndexNode.applyOnLeaf(segments[cacheSegment], cacheSegment, indexKey, segments[cacheSegment].rootReadLock(), readOperation);
      } finally {
         lock.unlockRead(stamp);
      }
   }

   /**
    * Get position or null if expired
    */
   public EntryPosition getPosition(Object key, int cacheSegment, org.infinispan.commons.io.ByteBuffer serializedKey) throws IOException {
      long stamp = lock.readLock();
      try {
         return IndexNode.applyOnLeaf(segments[cacheSegment], cacheSegment, toIndexKey(serializedKey), segments[cacheSegment].rootReadLock(), IndexNode.ReadOperation.GET_POSITION);
      } finally {
         lock.unlockRead(stamp);
      }
   }

   /**
    * Get position + numRecords, without expiration
    */
   public EntryInfo getInfo(Object key, int cacheSegment, byte[] serializedKey) throws IOException {
      long stamp = lock.readLock();
      try {
         return IndexNode.applyOnLeaf(segments[cacheSegment], cacheSegment, serializedKey, segments[cacheSegment].rootReadLock(), IndexNode.ReadOperation.GET_INFO);
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
      try {
         AggregateCompletionStage<Void> stage = CompletionStages.aggregateCompletionStage();
         for (FlowableProcessor<IndexRequest> processor : flowableProcessors) {
            // Ignore emptyFlowable as we use this to signal that we don't own that segment anymore
            if (processor == emptyFlowable) {
               continue;
            }
            IndexRequest clearRequest = IndexRequest.clearRequest();
            processor.onNext(clearRequest);
            stage.dependsOn(clearRequest);
         }

         return stage.freeze().whenComplete((ignore, t) -> {
            if (t != null) {
               log.clearError(t);
            } else {
               log.tracef("Clear has completed");
               // Clear is done, now purge the size segments
               for (int i = 0; i < sizePerSegment.length(); ++i) {
                  sizePerSegment.set(i, 0);
               }
            }
            // Unlock has to happen after clearing sizePerSegment to have it stay consistent
            lock.unlockWrite(writeStampToUnlock);
         });
      } catch (Throwable t) {
         lock.unlockWrite(writeStampToUnlock);
         log.debugf(t, "Clear encountered exception");
         throw t;
      }
   }

   public CompletionStage<Object> handleRequest(IndexRequest indexRequest) {
      flowableProcessors[indexRequest.getSegment()].onNext(indexRequest);
      return indexRequest;
   }

   public void ensureRunOnLast(Runnable runnable) {
      AtomicInteger count = new AtomicInteger(flowableProcessors.length);
      IndexRequest request = IndexRequest.syncRequest(() -> {
         if (count.decrementAndGet() == 0) {
            runnable.run();
         }
      });
      for (FlowableProcessor<IndexRequest> flowableProcessor : flowableProcessors) {
         flowableProcessor.onNext(request);
      }
   }

   public void deleteFileAsync(int fileId) {
      ensureRunOnLast(() -> {
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
      long maxSeqId = 0;
      long stamp = lock.readLock();
      try {
         for (Segment seg : segments) {
            maxSeqId = Math.max(maxSeqId, IndexNode.calculateMaxSeqId(seg, seg.rootReadLock()));
         }
      } finally {
         lock.unlockRead(stamp);
      }
      return maxSeqId;
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
                     segment.thenCompose(___ ->
                                 // Now we free all entries in the index, this will include expired and removed entries
                                 // Removed doesn't currently update free stats per ISPN-15246 - so we remove those as well
                                 segment.root.publish((keyAndMetadataRecord, leafNode, fileProvider, timeService) -> {
                                          compactor.free(leafNode.file, keyAndMetadataRecord.getHeader().totalLength());
                                          return null;
                                       }).ignoreElements()
                                       .toCompletionStage(null)
                           )
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

   static class Segment extends CompletableFuture<Void> implements Consumer<IndexRequest>, Action {
      final Index index;
      private final TemporaryTable temporaryTable;
      private final TreeMap<Short, List<IndexSpace>> freeBlocks = new TreeMap<>();
      private final ReadWriteLock rootLock = new ReentrantReadWriteLock();
      private final int id;
      private long indexFileSize = INDEX_FILE_HEADER_SIZE;

      private volatile IndexNode root;

      private Segment(Index index, int id, TemporaryTable temporaryTable) {
         this.index = index;
         this.temporaryTable = temporaryTable;

         this.id = id;

         // Just to init to empty
         root = IndexNode.emptyWithLeaves(this);
      }

      public int getId() {
         return id;
      }

      boolean load() throws IOException {
         int segmentMax = temporaryTable.getSegmentMax();
         FileProvider.Handle handle = index.indexFileProvider.getFile(id);
         try (handle) {
            ByteBuffer buffer = ByteBuffer.allocate(INDEX_FILE_HEADER_SIZE);
            boolean loaded;
            if (handle.getFileSize() >= INDEX_FILE_HEADER_SIZE && read(handle, buffer, 0)
                  && buffer.getInt(0) == GRACEFULLY && buffer.getInt(4) == segmentMax) {
               long rootOffset = buffer.getLong(8);
               short rootOccupied = buffer.getShort(16);
               long freeBlocksOffset = buffer.getLong(18);
               root = new IndexNode(this, rootOffset, rootOccupied);
               loadFreeBlocks(freeBlocksOffset);
               indexFileSize = freeBlocksOffset;
               loaded = true;
            } else {
               // If the file was empty and we had no entries in our sizePerSegment we can treat this as loaded
               // as this means we didn't own the segment before
               loaded = handle.getFileSize() == 0 && index.sizePerSegment.get(id) == 0;
               handle.truncate(0);
               root = IndexNode.emptyWithLeaves(this);
               // reserve space for shutdown
               indexFileSize = INDEX_FILE_HEADER_SIZE;
            }
            buffer.putInt(0, DIRTY);
            buffer.position(0);
            buffer.limit(4);
            write(handle, buffer, 0);

            return loaded;
         }
      }

      void delete() {
         // Empty segment is negative, so there is no file
         if (id >= 0) {
            log.tracef("Deleting file for index %s", id);
            index.indexFileProvider.deleteFile(id);
         }
      }

      void reset() throws IOException {
         try (FileProvider.Handle handle = index.indexFileProvider.getFile(id)) {
            handle.truncate(0);
            root = IndexNode.emptyWithLeaves(this);
            // reserve space for shutdown
            indexFileSize = INDEX_FILE_HEADER_SIZE;
            ByteBuffer buffer = ByteBuffer.allocate(INDEX_FILE_HEADER_SIZE);
            buffer.putInt(0, DIRTY);
            buffer.position(0);
            buffer.limit(4);

            write(handle, buffer, 0);
         }
         freeBlocks.clear();
      }

      @Override
      public void accept(IndexRequest request) throws Throwable {
         if (log.isTraceEnabled()) log.tracef("Indexing %s", request);
         IndexNode.OverwriteHook overwriteHook;
         IndexNode.RecordChange recordChange;
         switch (request.getType()) {
            case CLEAR:
               root = IndexNode.emptyWithLeaves(this);
               try (FileProvider.Handle handle = index.indexFileProvider.getFile(id)) {
                  handle.truncate(0);
               }
               indexFileSize = INDEX_FILE_HEADER_SIZE;
               freeBlocks.clear();
               index.nonBlockingManager.complete(request, null);
               return;
            case SYNC_REQUEST:
               Runnable runnable = (Runnable) request.getKey();
               runnable.run();
               index.nonBlockingManager.complete(request, null);
               return;
            case MOVED:
               recordChange = IndexNode.RecordChange.MOVE;
               overwriteHook = index.movedHook;
               break;
            case UPDATE:
               recordChange = IndexNode.RecordChange.INCREASE;
               overwriteHook = index.updateHook;
               break;
            case DROPPED:
               recordChange = IndexNode.RecordChange.DECREASE;
               overwriteHook = index.droppedHook;
               break;
            case FOUND_OLD:
               recordChange = IndexNode.RecordChange.INCREASE_FOR_OLD;
               overwriteHook = IndexNode.NOOP_HOOK;
               break;
            default:
               throw new IllegalArgumentException(request.toString());
         }
         try {
            IndexNode.setPosition(root, request, overwriteHook, recordChange);
         } catch (Throwable e) {
            request.completeExceptionally(e);
         }
         temporaryTable.removeConditionally(request.getSegment(), request.getKey(), request.getFile(), request.getOffset());
         if (request.getType() != IndexRequest.Type.UPDATE) {
            // The update type will complete it in the switch statement above
            index.nonBlockingManager.complete(request, null);
         }
      }

      // This is ran when the flowable ends either via normal termination or error
      @Override
      public void run() throws IOException {
         try {
            IndexSpace rootSpace = allocateIndexSpace(root.length());
            root.store(rootSpace);
            try (FileProvider.Handle handle = index.indexFileProvider.getFile(id)) {
               ByteBuffer buffer = ByteBuffer.allocate(4);
               buffer.putInt(0, freeBlocks.size());
               long offset = indexFileSize;
               offset += write(handle, buffer, offset);
               for (Map.Entry<Short, List<IndexSpace>> entry : freeBlocks.entrySet()) {
                  List<IndexSpace> list = entry.getValue();
                  int requiredSize = 8 + list.size() * 10;
                  buffer = buffer.capacity() < requiredSize ? ByteBuffer.allocate(requiredSize) : buffer;
                  buffer.position(0);
                  buffer.limit(requiredSize);
                  // TODO: change this to short
                  buffer.putInt(entry.getKey());
                  buffer.putInt(list.size());
                  for (IndexSpace space : list) {
                     buffer.putLong(space.offset);
                     buffer.putShort(space.length);
                  }
                  buffer.flip();
                  offset += write(handle, buffer, offset);
               }
               int headerWithoutMagic = INDEX_FILE_HEADER_SIZE - 4;
               buffer = buffer.capacity() < headerWithoutMagic ? ByteBuffer.allocate(headerWithoutMagic) : buffer;
               buffer.position(0);
               // we need to set limit ahead, otherwise the putLong could throw IndexOutOfBoundsException
               buffer.limit(headerWithoutMagic);
               buffer.putInt(index.segments.length);
               buffer.putLong(rootSpace.offset);
               buffer.putShort(rootSpace.length);
               buffer.putLong(indexFileSize);
               buffer.flip();
               write(handle, buffer, 4);

               buffer.position(0);
               buffer.limit(4);
               buffer.putInt(0, GRACEFULLY);
               write(handle, buffer, 0);
            }

            complete(null);
         } catch (Throwable t) {
            completeExceptionally(t);
         }
      }

      private void loadFreeBlocks(long freeBlocksOffset) throws IOException {
         ByteBuffer buffer = ByteBuffer.allocate(8);
         buffer.limit(4);
         long offset = freeBlocksOffset;
         try (FileProvider.Handle handle = index.indexFileProvider.getFile(id)) {
            if (!read(handle, buffer, offset)) {
               throw new IOException("Cannot read free blocks lists!");
            }
            offset += 4;
            int numLists = buffer.getInt(0);
            for (int i = 0; i < numLists; ++i) {
               buffer.position(0);
               buffer.limit(8);
               if (!read(handle, buffer, offset)) {
                  throw new IOException("Cannot read free blocks lists!");
               }
               offset += 8;
               // TODO: change this to short
               int blockLength = buffer.getInt(0);
               assert blockLength <= Short.MAX_VALUE;
               int listSize = buffer.getInt(4);
               // Ignore any free block that had no entries as it adds time complexity to our lookup
               if (listSize > 0) {
                  int requiredSize = 10 * listSize;
                  buffer = buffer.capacity() < requiredSize ? ByteBuffer.allocate(requiredSize) : buffer;
                  buffer.position(0);
                  buffer.limit(requiredSize);
                  if (!read(handle, buffer, offset)) {
                     throw new IOException("Cannot read free blocks lists!");
                  }
                  offset += requiredSize;
                  buffer.flip();
                  ArrayList<IndexSpace> list = new ArrayList<>(listSize);
                  for (int j = 0; j < listSize; ++j) {
                     list.add(new IndexSpace(buffer.getLong(), buffer.getShort()));
                  }
                  freeBlocks.put((short) blockLength, list);
               }
            }
         }
      }

      public FileProvider.Handle getIndexFile() throws IOException {
         return index.indexFileProvider.getFile(id);
      }

      public void forceIndexIfOpen(boolean metaData) throws IOException {
         FileProvider.Handle handle = index.indexFileProvider.getFileIfOpen(id);
         if (handle != null) {
            try (handle) {
               handle.force(metaData);
            }
         }
      }

      public FileProvider getFileProvider() {
         return index.dataFileProvider;
      }

      public Compactor getCompactor() {
         return index.compactor;
      }

      public IndexNode getRoot() {
         // this has to be called with rootLock locked!
         return root;
      }

      public void setRoot(IndexNode root) {
         rootLock.writeLock().lock();
         this.root = root;
         rootLock.writeLock().unlock();
      }

      public int getMaxNodeSize() {
         return index.maxNodeSize;
      }

      public int getMinNodeSize() {
         return index.minNodeSize;
      }

      // this should be accessed only from the updater thread
      IndexSpace allocateIndexSpace(short length) {
         // Use tailMap so that we only require O(logN) to find the iterator
         // This avoids an additional O(logN) to do an entry removal
         Iterator<Map.Entry<Short, List<IndexSpace>>> iter = freeBlocks.tailMap(length).entrySet().iterator();
         while (iter.hasNext()) {
            Map.Entry<Short, List<IndexSpace>> entry = iter.next();
            short spaceLength = entry.getKey();
            // Only use the space if it is only 25% larger to avoid too much fragmentation
            if ((length + (length >> 2)) < spaceLength) {
               break;
            }
            List<IndexSpace> list = entry.getValue();
            if (!list.isEmpty()) {
               IndexSpace spaceToReturn = list.remove(list.size() - 1);
               if (list.isEmpty()) {
                  iter.remove();
               }
               return spaceToReturn;
            }
            iter.remove();
         }
         long oldSize = indexFileSize;
         indexFileSize += length;
         return new IndexSpace(oldSize, length);
      }

      // this should be accessed only from the updater thread
      void freeIndexSpace(long offset, short length) {
         if (length <= 0) throw new IllegalArgumentException("Offset=" + offset + ", length=" + length);
         // TODO: fragmentation!
         // TODO: memory bounds!
         if (offset + length < indexFileSize) {
            freeBlocks.computeIfAbsent(length, k -> new ArrayList<>()).add(new IndexSpace(offset, length));
         } else {
            indexFileSize -= length;
            try (FileProvider.Handle handle = index.indexFileProvider.getFile(id)) {
               handle.truncate(indexFileSize);
            } catch (IOException e) {
               log.cannotTruncateIndex(e);
            }
         }
      }

      Lock rootReadLock() {
         return rootLock.readLock();
      }

      public TimeService getTimeService() {
         return index.timeService;
      }
   }

   /**
    * Offset-length pair
    */
   static class IndexSpace {
      protected long offset;
      protected short length;

      IndexSpace(long offset, short length) {
         this.offset = offset;
         this.length = length;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof IndexSpace)) return false;

         IndexSpace innerNode = (IndexSpace) o;

         return length == innerNode.length && offset == innerNode.offset;
      }

      @Override
      public int hashCode() {
         int result = (int) (offset ^ (offset >>> 32));
         result = 31 * result + length;
         return result;
      }

      @Override
      public String toString() {
         return String.format("[%d-%d(%d)]", offset, offset + length, length);
      }
   }

   <V> Flowable<EntryRecord> publish(IntSet cacheSegments, boolean loadValues) {
      return Flowable.fromIterable(cacheSegments)
            .concatMap(cacheSegment -> publish(cacheSegment, loadValues));
   }

   Flowable<EntryRecord> publish(int cacheSegment, boolean loadValues) {
      long stamp = lock.readLock();
      try {
         var segment = segments[cacheSegment];
         if (segment.index.sizePerSegment.get(cacheSegment) == 0) {
            lock.unlockRead(stamp);
            return Flowable.empty();
         }
         return segment.root.publish((keyAndMetadataRecord, leafNode, fileProvider, currentTime) -> {
            long expiryTime = keyAndMetadataRecord.getHeader().expiryTime();
            // Ignore any key or value if it is expired or was removed
            if (expiryTime > 0 && expiryTime < currentTime || keyAndMetadataRecord.getHeader().valueLength() <= 0) {
               return null;
            }
            if (loadValues) {
               log.tracef("Loading value record for leafNode: %s", leafNode);

               return leafNode.loadValue(keyAndMetadataRecord, fileProvider);
            }
            return keyAndMetadataRecord;
         }).doFinally(() -> lock.unlockRead(stamp));
      } catch (Throwable t) {
         lock.unlockRead(stamp);
         throw t;
      }
   }
}
