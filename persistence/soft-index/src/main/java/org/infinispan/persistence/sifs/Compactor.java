package org.infinispan.persistence.sifs;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.reactive.RxJavaInterop;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * Component keeping the data about log file usage - as soon as entries from some file are overwritten so that the file
 * becomes cluttered with old records, the valid records are moved to another file and the old ones are dropped.
 * Expired records are moved as tombstones without values (records of entry removal).
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class Compactor implements Consumer<Object> {
   private static final Log log = LogFactory.getLog(Compactor.class, Log.class);

   private final ConcurrentMap<Integer, Stats> fileStats = new ConcurrentHashMap<>();
   private final FileProvider fileProvider;
   private final TemporaryTable temporaryTable;
   private final Marshaller marshaller;
   private final TimeService timeService;
   private final KeyPartitioner keyPartitioner;
   private final int maxFileSize;
   private final double compactionThreshold;
   private final FlowableProcessor<Object> processor;

   private Index index;
   // as processing single scheduled compaction takes a lot of time, we don't use the queue to signalize
   private final AtomicInteger clearSignal = new AtomicInteger();
   private volatile boolean terminateSignal = false;

   private CompletableFuture<Void> paused = CompletableFutures.completedNull();

   private static final Object RESUME_PILL = new Object();

   FileProvider.Log logFile = null;
   int currentOffset = 0;

   public Compactor(FileProvider fileProvider, TemporaryTable temporaryTable, Marshaller marshaller,
                    TimeService timeService, KeyPartitioner keyPartitioner, int maxFileSize,
                    double compactionThreshold, Executor blockingExecutor) {
      this.fileProvider = fileProvider;
      this.temporaryTable = temporaryTable;
      this.marshaller = marshaller;
      this.timeService = timeService;
      this.keyPartitioner = keyPartitioner;
      this.maxFileSize = maxFileSize;
      this.compactionThreshold = compactionThreshold;

      processor = UnicastProcessor.create();
      Scheduler scheduler = Schedulers.from(blockingExecutor);
      processor.observeOn(scheduler)
            .delay(obj -> {
               // These types are special and should allow processing always
               if (obj == RESUME_PILL || obj instanceof CompletableFuture) {
                  return Flowable.empty();
               }
               return RxJavaInterop.voidCompletionStageToFlowable(paused);
            })
            .subscribe(this, error -> log.warn("Compactor encountered an exception", error));
   }

   public void setIndex(Index index) {
      this.index = index;
   }

   public void releaseStats(int file) {
      fileStats.remove(file);
   }

   public void free(int file, int size) {
      // entries expired from compacted file are reported with file = -1
      if (file < 0) return;
      recordFreeSpace(getStats(file), file, size);
   }

   public void completeFile(int file) {
      Stats stats = getStats(file);
      stats.setCompleted();
      if (stats.readyToBeScheduled(compactionThreshold, stats.getFree())) {
         schedule(file, stats);
      }
   }

   private Stats getStats(int file) {
      Stats stats = fileStats.get(file);
      if (stats == null) {
         int fileSize = (int) fileProvider.getFileSize(file);
         stats = new Stats(fileSize, 0);
         Stats other = fileStats.putIfAbsent(file, stats);
         if (other != null) {
            if (fileSize > other.getTotal()) {
               other.setTotal(fileSize);
            }
            return other;
         }
      }
      if (stats.getTotal() < 0) {
         int fileSize = (int) fileProvider.getFileSize(file);
         if (fileSize >= 0) {
            stats.setTotal(fileSize);
         }
      }
      return stats;
   }

   private void recordFreeSpace(Stats stats, int file, int size) {
      if (stats.addFree(size, compactionThreshold)) {
         schedule(file, stats);
      }
   }

   private void schedule(int file, Stats stats) {
      boolean shouldSchedule = false;
      synchronized (stats) {
         if (!stats.isScheduled()) {
            log.debug(String.format("Scheduling file %d for compaction: %d/%d free", file, stats.free.get(), stats.total));
            stats.setScheduled();
            shouldSchedule = true;
         }
      }
      if (shouldSchedule) {
         processor.onNext(file);
      }
   }

   /**
    * Immediately sends a request to pause the compactor. The returned stage will complete when the
    * compactor is actually paused. To resume the compactor the {@link #resumeAfterPause()} method
    * must be invoked or else the compactor will not process new requests.
    * @return a stage that when complete the compactor is paused
    */
   public CompletionStage<Void> clearAndPause() {
      clearSignal.incrementAndGet();
      CompletableFuture<Void> clearFuture = new CompletableFuture<>();
      // Make sure to do this before submitting to processor this is done in the blocking thread
      clearFuture.whenComplete((ignore, t) -> fileStats.clear());
      processor.onNext(clearFuture);
      return clearFuture;
   }

   public void resumeAfterPause() {
      processor.onNext(RESUME_PILL);
   }

   public void stopOperations() {
      terminateSignal = true;
      processor.onComplete();
   }

   @Override
   public void accept(Object o) throws Throwable {
      if (terminateSignal) {
         // Just ignore if terminated or was a resume
         return;
      }
      if (o == RESUME_PILL) {
         // This completion will push all the other tasks that have been delayed in this method call
         paused.complete(null);
         return;
      }
      // Note that this accept is only invoked from a single thread at a time so we don't have to worry about
      // any other threads decrementing clear signal. However another thread can increment, that is okay for us
      if (clearSignal.get() > 0) {
         // We ignore any entries since it was last cleared
         if (o instanceof CompletableFuture) {
            clearSignal.decrementAndGet();
            // After clearing we have to pause until we get a RESUME_PILL
            paused = new CompletableFuture<>();
            ((CompletableFuture<?>) o).complete(null);

            if (logFile != null) {
               logFile.close();
               completeFile(logFile.fileId);
               logFile = null;
            }
         }
         return;
      }

      // Any other type submitted has to be a positive integer
      int scheduledFile = (int) o;
      assert scheduledFile >= 0;
      log.debugf("Compacting file %d", scheduledFile);
      int scheduledOffset = 0;
      FileProvider.Handle handle = fileProvider.getFile(scheduledFile);
      if (handle == null) {
         throw new IllegalStateException("Compactor should not get deleted file for compaction!");
      }
      try {
         AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
         while (clearSignal.get() == 0 && !terminateSignal) {
            EntryHeader header = EntryRecord.readEntryHeader(handle, scheduledOffset);
            if (header == null) {
               break;
            }
            byte[] serializedKey = EntryRecord.readKey(handle, header, scheduledOffset);
            if (serializedKey == null) {
               throw new IllegalStateException("End of file reached when reading key on "
                     + handle.getFileId() + ":" + scheduledOffset);
            }
            Object key = marshaller.objectFromByteBuffer(serializedKey);
            int segment = keyPartitioner.getSegment(key);

            int indexedOffset = header.valueLength() > 0 ? scheduledOffset : ~scheduledOffset;
            boolean drop = true;
            boolean truncate = false;
            EntryPosition entry = temporaryTable.get(segment, key);
            if (entry != null) {
               synchronized (entry) {
                  if (log.isTraceEnabled()) {
                     log.tracef("Key for %d:%d was found in temporary table on %d:%d",
                           scheduledFile, scheduledOffset, entry.file, entry.offset);
                  }
                  if (entry.file == scheduledFile && entry.offset == indexedOffset) {
                     // It's quite unlikely that we would compact a record that is not indexed yet,
                     // but let's handle that
                     if (header.expiryTime() >= 0 && header.expiryTime() <= timeService.wallClockTime()) {
                        truncate = true;
                     }
                  } else {
                     truncate = true;
                  }
               }
               // When we have found the entry in temporary table, it's possible that the delete operation
               // (that was recorded in temporary table) will arrive to index after DROPPED - in that case
               // we could remove the entry and delete would not find it
               drop = false;
            } else {
               EntryInfo info = index.getInfo(key, serializedKey);
               assert info != null : String.format("Index does not recognize entry on %d:%d");
               assert info.numRecords > 0;
               if (info.file == scheduledFile && info.offset == scheduledOffset) {
                  assert header.valueLength() > 0;
                  // live record with data
                  truncate = header.expiryTime() >= 0 && header.expiryTime() <= timeService.wallClockTime();
                  if (log.isTraceEnabled()) {
                     log.tracef("Is %d:%d expired? %s, numRecords? %d", scheduledFile, scheduledOffset, truncate, info.numRecords);
                  }
                  if (!truncate || info.numRecords > 1) {
                     drop = false;
                  }
                  // Drop only when it is expired and has single record
               } else if (info.file == scheduledFile && info.offset == ~scheduledOffset && info.numRecords > 1) {
                  // just tombstone but there are more non-compacted records for this key so we have to keep it
                  drop = false;
               } else if (log.isTraceEnabled()) {
                  log.tracef("Key for %d:%d was found in index on %d:%d, %d record => drop",
                        scheduledFile, scheduledOffset, info.file, info.offset, info.numRecords);
               }
            }
            if (drop) {
               if (log.isTraceEnabled()) {
                  log.tracef("Drop %d:%d (%s)", scheduledFile, (Object)scheduledOffset,
                        header.valueLength() > 0 ? "record" : "tombstone");
               }
               index.handleRequest(IndexRequest.dropped(segment, key, serializedKey, scheduledFile, scheduledOffset));
            } else {
               if (logFile == null || currentOffset + header.totalLength() > maxFileSize) {
                  if (logFile != null) {
                     logFile.close();
                     completeFile(logFile.fileId);
                  }
                  currentOffset = 0;
                  logFile = fileProvider.getFileForLog();
                  log.debugf("Compacting to %d", (Object) logFile.fileId);
               }

               byte[] serializedValue = null;
               EntryMetadata metadata = null;
               byte[] serializedInternalMetadata = null;
               int entryOffset;
               int writtenLength;
               if (header.valueLength() > 0 && !truncate) {
                  if (header.metadataLength() > 0) {
                     metadata = EntryRecord.readMetadata(handle, header, scheduledOffset);
                  }
                  serializedValue = EntryRecord.readValue(handle, header, scheduledOffset);
                  if (header.internalMetadataLength() > 0) {
                     serializedInternalMetadata = EntryRecord.readInternalMetadata(handle, header, scheduledOffset);
                  }
                  entryOffset = currentOffset;
                  writtenLength = header.totalLength();
               } else {
                  entryOffset = ~currentOffset;
                  writtenLength = header.getHeaderLength() + header.keyLength();
               }
               if (logFile.fileId == 4 && entryOffset == 176) {
                  System.currentTimeMillis();
               }
               EntryRecord.writeEntry(logFile.fileChannel, serializedKey, metadata, serializedValue, serializedInternalMetadata, header.seqId(), header.expiryTime());
               TemporaryTable.LockedEntry lockedEntry = temporaryTable.replaceOrLock(segment, key, logFile.fileId, entryOffset, scheduledFile, indexedOffset);
               if (lockedEntry == null) {
                  if (log.isTraceEnabled()) {
                     log.trace("Found entry in temporary table");
                  }
               } else {
                  boolean update = false;
                  try {
                     EntryInfo info = index.getInfo(key, serializedKey);
                     if (info == null) {
                        throw new IllegalStateException(String.format(
                              "%s was not found in index but it was not in temporary table and there's entry on %d:%d", key, scheduledFile, indexedOffset));
                     } else {
                        update = info.file == scheduledFile && info.offset == indexedOffset;
                     }
                     if (log.isTraceEnabled()) {
                        log.tracef("In index the key is on %d:%d (%s)", info.file, info.offset, String.valueOf(update));
                     }
                  } finally {
                     if (update) {
                        temporaryTable.updateAndUnlock(lockedEntry, logFile.fileId, entryOffset);
                     } else {
                        temporaryTable.removeAndUnlock(lockedEntry, segment, key);
                     }
                  }
               }
               if (log.isTraceEnabled()) {
                  log.tracef("Update %d:%d -> %d:%d | %d,%d", scheduledFile, indexedOffset,
                        logFile.fileId, entryOffset, logFile.fileChannel.position(), logFile.fileChannel.size());
               }
               // entryFile cannot be used as we have to report the file due to free space statistics
               IndexRequest moveRequest = IndexRequest.moved(segment, key, serializedKey, logFile.fileId, entryOffset, writtenLength,
                     scheduledFile, indexedOffset);
               index.handleRequest(moveRequest);
               aggregateCompletionStage.dependsOn(moveRequest);

               currentOffset += writtenLength;
            }
            scheduledOffset += header.totalLength();
         }
         // We delay the next operation until all prior moves are done. By moving it can trigger another
         // compaction before the index has been fully updated. Thus we block any other compaction events
         // until all entries have been moved for this file
         paused = aggregateCompletionStage.freeze().toCompletableFuture();
      } finally {
         handle.close();
      }
      if (!terminateSignal && clearSignal.get() == 0) {
         // The deletion must be executed only after the index is fully updated.
         log.debugf("Finished compacting %d, scheduling delete", scheduledFile);
         index.deleteFileAsync(scheduledFile);
      }
   }

   private static class Stats {
      private final AtomicInteger free;
      private volatile int total;
      /* File is not 'completed' when we have not loaded that yet completely.
         Files created by log appender/compactor are completed as soon as it closes them.
         File cannot be scheduled for compaction until it's completed.
         */
      private volatile boolean completed = false;
      private volatile boolean scheduled = false;

      private Stats(int total, int free) {
         this.free = new AtomicInteger(free);
         this.total = total;
      }

      public int getTotal() {
         return total;
      }

      public void setTotal(int total) {
         this.total = total;
      }

      public boolean addFree(int size, double compactionThreshold) {
         int free = this.free.addAndGet(size);
         return readyToBeScheduled(compactionThreshold, free);
      }

      public int getFree() {
         return free.get();
      }

      public boolean readyToBeScheduled(double compactionThreshold, int free) {
         int total = this.total;
         return completed && !scheduled && total >= 0 && free > total * compactionThreshold;
      }

      public boolean isScheduled() {
         return scheduled;
      }

      public void setScheduled() {
         scheduled = true;
      }

      public boolean isCompleted() {
         return completed;
      }

      public void setCompleted() {
         this.completed = true;
      }
   }
}
