package org.infinispan.persistence.sifs;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.ExpiryHelper;
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

   // This buffer is used by the compactor thread to avoid allocating buffers per entry written that are smaller
   // than the header size
   private final java.nio.ByteBuffer REUSED_BUFFER = java.nio.ByteBuffer.allocate(EntryHeader.HEADER_SIZE_11_0);

   FileProvider.Log logFile = null;
   long nextExpirationTime = -1;
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
      recordFreeSpace(getStats(file, -1, -1), file, size);
   }

   public void completeFile(int file, int currentSize, long nextExpirationTime) {
      Stats stats = getStats(file, currentSize, nextExpirationTime);
      stats.setCompleted();
      if (stats.readyToBeScheduled(compactionThreshold, stats.getFree())) {
         schedule(file, stats);
      }
   }

   public interface CompactionExpirationSubscriber {
      void onEntryPosition(EntryPosition entryPosition) throws IOException;

      void onEntryEntryRecord(EntryRecord entryRecord) throws IOException;

      void onComplete();

      void onError(Throwable t);
   }


   public void performExpirationCompaction(CompactionExpirationSubscriber subscriber) {
      processor.onNext(subscriber);
   }

   private Stats getStats(int file, int currentSize, long expirationTime) {
      Stats stats = fileStats.get(file);
      if (stats == null) {
         int fileSize = currentSize < 0 ? (int) fileProvider.getFileSize(file) : currentSize;
         stats = new Stats(fileSize, 0, expirationTime);
         Stats other = fileStats.putIfAbsent(file, stats);
         if (other != null) {
            if (fileSize > other.getTotal()) {
               other.setTotal(fileSize);
            }
            return other;
         }
      }
      if (stats.getTotal() < 0) {
         int fileSize = currentSize < 0 ? (int) fileProvider.getFileSize(file) : currentSize;
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
            log.debugf("Scheduling file %d for compaction: %d/%d free", file, stats.free.get(), stats.total);
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
               completeFile(logFile.fileId, currentOffset, nextExpirationTime);
               logFile = null;
               nextExpirationTime = -1;
            }
         }
         return;
      }

      if (o instanceof CompactionExpirationSubscriber) {
         CompactionExpirationSubscriber subscriber = (CompactionExpirationSubscriber) o;
         try {
            for (CloseableIterator<Integer> iter = fileProvider.getFileIterator(); iter.hasNext(); ) {
               int fileId = iter.next();
               Stats stats = fileStats.get(fileId);
               long currentTimeMilliseconds = timeService.wallClockTime();
               boolean isLogFile = fileProvider.isLogFile(fileId);
               // Only run expiration compaction on files that have expired entries or log files, which doesn't have a expiration time.
               // If there is no stat for the file it also means it is a log file
               if (stats == null || isLogFile || stats.nextExpirationTime < currentTimeMilliseconds) {
                  // We cannot compact a file that is currently being logged to... yet
                  compactSingleFile(fileId, isLogFile, subscriber, currentTimeMilliseconds);
               }
            }
            subscriber.onComplete();
         } catch (Throwable t) {
            subscriber.onError(t);
         }
         return;
      }

      // Any other type submitted has to be a positive integer
      compactSingleFile((int) o, false, null, timeService.wallClockTime());
   }

   /**
    * Compacts a single file into the current log file. This method has two modes of operation based on if the file
    * is a log file or not. If it is a log file non expired entries are ignored and only expired entries are "updated"
    * to be deleted in the new log file and expiration listener is notified. If it is not a log file all entries are
    * moved to the new log file and the current file is deleted afterwards. If an expired entry is found during compaction
    * of a non log file the expiration listener is notified and the entry is not moved, however if no expiration listener
    * is provided the expired entry is moved to the new file as is still expired.
    * @param scheduledFile the file identifier to compact
    * @param isLogFile     whether the provided file as a log file, which means we only notify and compact expired
    *                      entries (ignore others)
    * @param subscriber    the subscriber that is notified of various entries being expired
    * @throws IOException            thrown if there was an issue with reading or writing to a file
    * @throws ClassNotFoundException thrown if there is an issue deserializing the key for an entry
    */
   private void compactSingleFile(int scheduledFile, boolean isLogFile, CompactionExpirationSubscriber subscriber,
         long currentTimeMilliseconds) throws IOException, ClassNotFoundException {
      assert scheduledFile >= 0;
      log.tracef("Compacting file %d", scheduledFile);
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

            int valueLength = header.valueLength();
            int indexedOffset = valueLength > 0 ? scheduledOffset : ~scheduledOffset;
            // Whether to drop the entire index (this cannot be true if truncate is false)
            boolean drop = true;
            // Whether to truncate the value
            boolean truncate = false;
            // Whether the entry when truncating was due to expiration (this cannot be true if truncate is false)
            EntryPosition entry = temporaryTable.get(segment, key);
            if (entry != null) {
               synchronized (entry) {
                  if (log.isTraceEnabled()) {
                     log.tracef("Key for %d:%d was found in temporary table on %d:%d",
                           scheduledFile, scheduledOffset, entry.file, entry.offset);
                  }
                  if (entry.file == scheduledFile && entry.offset == indexedOffset) {
                     long entryExpiryTime = header.expiryTime();
                     // It's quite unlikely that we would compact a record that is not indexed yet,
                     // but let's handle that
                     if (entryExpiryTime >= 0 && entryExpiryTime <= currentTimeMilliseconds) {
                        // We can only truncate expired entries if this was compacted with purge expire
                        if (subscriber != null) {
                           truncate = true;
                           subscriber.onEntryPosition(entry);
                        }
                     } else if (isLogFile) {
                        // Non expired entry in a log file, just skip it
                        scheduledOffset += header.totalLength();
                        continue;
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
               assert info != null;
               assert info.numRecords > 0;
               if (info.file == scheduledFile && info.offset == scheduledOffset) {
                  assert header.valueLength() > 0;
                  long entryExpiryTime = header.expiryTime();
                  // live record with data
                  if (entryExpiryTime >= 0 && entryExpiryTime <= currentTimeMilliseconds) {
                     EntryRecord record = index.getRecordEvenIfExpired(key, serializedKey);
                     // We can only truncate expired entries if this was compacted with purge expire
                     if (subscriber != null) {
                        truncate = true;
                        subscriber.onEntryEntryRecord(record);
                        // If there are more entries we cannot drop the index as we need a tombstone
                        if (info.numRecords > 1) {
                           drop = false;
                        }
                     } else {
                        drop = false;
                     }
                  } else if (isLogFile) {
                     // Non expired entry in a log file, just skip it
                     scheduledOffset += header.totalLength();
                     continue;
                  } else {
                     drop = false;
                  }

                  if (log.isTraceEnabled()) {
                     log.tracef("Is %d:%d expired? %s, numRecords? %d", scheduledFile, scheduledOffset, truncate, info.numRecords);
                  }
               } else if (info.file == scheduledFile && info.offset == ~scheduledOffset && info.numRecords > 1) {
                  // The entry was expired, but we have other records so we can't drop this one or else the index will rebuild incorrectly
                  drop = false;
               } else if (log.isTraceEnabled()) {
                  log.tracef("Key for %d:%d was found in index on %d:%d, %d record => drop",
                        scheduledFile, scheduledOffset, info.file, info.offset, info.numRecords);
               }
            }

            if (drop) {
               if (log.isTraceEnabled()) {
                  log.tracef("Drop %d:%d (%s)", scheduledFile, (Object) scheduledOffset,
                        header.valueLength() > 0 ? "record" : "tombstone");
               }
               index.handleRequest(IndexRequest.dropped(segment, key, serializedKey, scheduledFile, scheduledOffset));
            } else {
               if (logFile == null || currentOffset + header.totalLength() > maxFileSize) {
                  if (logFile != null) {
                     logFile.close();
                     completeFile(logFile.fileId, currentOffset, nextExpirationTime);
                     nextExpirationTime = -1;
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
               nextExpirationTime = ExpiryHelper.mostRecentExpirationTime(nextExpirationTime, header.expiryTime());
               EntryRecord.writeEntry(logFile.fileChannel, REUSED_BUFFER, serializedKey, metadata, serializedValue, serializedInternalMetadata, header.seqId(), header.expiryTime());
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
               IndexRequest indexRequest;
               if (isLogFile) {
                  // When it is a log file we are still keeping the original entry, we are just updating it to say
                  // it was expired
                  indexRequest = IndexRequest.update(segment, key, serializedKey, logFile.fileId, entryOffset, writtenLength);
               } else {
                  // entryFile cannot be used as we have to report the file due to free space statistics
                  indexRequest = IndexRequest.moved(segment, key, serializedKey, logFile.fileId, entryOffset, writtenLength,
                        scheduledFile, indexedOffset);
               }
               index.handleRequest(indexRequest);
               aggregateCompletionStage.dependsOn(indexRequest);

               currentOffset += writtenLength;
            }
            scheduledOffset += header.totalLength();
         }
         // We delay the next operation until all prior moves are done. By moving it can trigger another
         // compaction before the index has been fully updated. Thus we block any other compaction events
         // until all entries have been moved for this file
         CompletionStage<Void> aggregate = aggregateCompletionStage.freeze();
         paused = new CompletableFuture<>();
         // We resume after completed, Note that we must complete the {@code paused} variable inside the compactor
         // execution pipeline otherwise we can invoke compactor operations in the wrong thread
         aggregate.whenComplete((ignore, t) -> {
            resumeAfterPause();
            if (t != null) {
               log.error("There was a problem moving indexes for compactor with file " + logFile.fileId, t);
            }
         });
      } finally {
         handle.close();
      }
      if (isLogFile) {
         log.tracef("Finished expiring entries in log file %d, leaving file as is", scheduledFile);
      } else if (terminateSignal && clearSignal.get() == 0) {
         // The deletion must be executed only after the index is fully updated.
         log.tracef("Finished compacting %d, scheduling delete", scheduledFile);
         index.deleteFileAsync(scheduledFile);
      }
   }


   private static class Stats {
      private final AtomicInteger free;
      private volatile int total;
      private final long nextExpirationTime;
      /* File is not 'completed' when we have not loaded that yet completely.
         Files created by log appender/compactor are completed as soon as it closes them.
         File cannot be scheduled for compaction until it's completed.
         */
      private volatile boolean completed = false;
      private volatile boolean scheduled = false;

      private Stats(int total, int free, long nextExpirationTime) {
         this.free = new AtomicInteger(free);
         this.total = total;
         this.nextExpirationTime = nextExpirationTime;
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
         return completed && !scheduled && total >= 0 && free >= total * compactionThreshold;
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
