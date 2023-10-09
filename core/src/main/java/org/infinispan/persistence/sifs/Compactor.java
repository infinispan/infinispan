package org.infinispan.persistence.sifs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.reactive.RxJavaInterop;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.NonBlockingManager;
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

   private final NonBlockingManager nonBlockingManager;
   private final ConcurrentMap<Integer, Stats> fileStats = new ConcurrentHashMap<>();
   private final FileProvider fileProvider;
   private final TemporaryTable temporaryTable;
   private final Marshaller marshaller;
   private final TimeService timeService;
   private final KeyPartitioner keyPartitioner;
   private final int maxFileSize;
   private final double compactionThreshold;
   private final Executor blockingExecutor;

   // Initialize so we can enqueue operations until start begins
   private FlowableProcessor<Object> processor = UnicastProcessor.create().toSerialized();

   private Index index;
   // as processing single scheduled compaction takes a lot of time, we don't use the queue to signalize
   private final AtomicBoolean clearSignal = new AtomicBoolean();
   private volatile boolean terminateSignal = false;
   // variable used to denote running (not null but not complete) and stopped (not null but complete)
   // This variable is never to be null
   private volatile CompletableFuture<?> stopped = CompletableFutures.completedNull();

   private CompletableFuture<Void> paused = CompletableFutures.completedNull();

   // Special object used solely for the purpose of resuming the compactor after compacting a file and waiting for
   // all indices to be updated
   private static final Object RESUME_PILL = new Object();

   // This buffer is used by the compactor thread to avoid allocating buffers per entry written that are smaller
   // than the header size
   private final java.nio.ByteBuffer REUSED_BUFFER = java.nio.ByteBuffer.allocate(EntryHeader.HEADER_SIZE_11_0);

   FileProvider.Log logFile = null;
   long nextExpirationTime = -1;
   int currentOffset = 0;

   public Compactor(NonBlockingManager nonBlockingManager, FileProvider fileProvider, TemporaryTable temporaryTable,
         Marshaller marshaller, TimeService timeService, KeyPartitioner keyPartitioner, int maxFileSize,
         double compactionThreshold, Executor blockingExecutor) {
      this.nonBlockingManager = nonBlockingManager;
      this.fileProvider = fileProvider;
      this.temporaryTable = temporaryTable;
      this.marshaller = marshaller;
      this.timeService = timeService;
      this.keyPartitioner = keyPartitioner;
      this.maxFileSize = maxFileSize;
      this.compactionThreshold = compactionThreshold;
      this.blockingExecutor = blockingExecutor;
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
      completeFile(file, currentSize, nextExpirationTime, true);
   }

   public void completeFile(int file, int currentSize, long nextExpirationTime, boolean canSchedule) {
      Stats stats = getStats(file, currentSize, nextExpirationTime);
      stats.setCompleted();
      // It is possible this was a logFile that was compacted
      if (canSchedule && stats.readyToBeScheduled(compactionThreshold, stats.getFree())) {
         schedule(file, stats);
      }
   }

   ConcurrentMap<Integer, Stats> getFileStats() {
      return fileStats;
   }

   boolean addFreeFile(int file, int expectedSize, int freeSize, long expirationTime) {
      return addFreeFile(file, expectedSize, freeSize, expirationTime, true);
   }

   boolean addFreeFile(int file, int expectedSize, int freeSize, long expirationTime, boolean canScheduleCompaction) {
      int fileSize = (int) fileProvider.getFileSize(file);
      if (fileSize != expectedSize) {
         log.tracef("Unable to add file %s as it its size %s does not match expected %s, index may be dirty", file, fileSize, expectedSize);
         return false;
      }
      Stats stats = new Stats(fileSize, freeSize, expirationTime);

      if (fileStats.putIfAbsent(file, stats) != null) {
         log.tracef("Unable to add file %s as it is already present, index may be dirty", file);
         return false;
      }
      log.tracef("Added new file %s to compactor manually with total size %s and free size %s", file, fileSize, freeSize);
      stats.setCompleted();
      if (canScheduleCompaction && stats.readyToBeScheduled(compactionThreshold, freeSize)) {
         schedule(file, stats);
      }
      return true;
   }

   public void start() {
      stopped = new CompletableFuture<>();

      Scheduler scheduler = Schedulers.from(blockingExecutor);
      processor.observeOn(scheduler)
            .delay(obj -> {
               // These types are special and should allow processing always
               if (obj == RESUME_PILL || obj instanceof CompletableFuture) {
                  return Flowable.empty();
               }
               return RxJavaInterop.voidCompletionStageToFlowable(paused);
            })
            .subscribe(this, error -> {
               log.compactorEncounteredException(error, -1);
               stopped.completeExceptionally(error);
            }, () -> stopped.complete(null));

      fileStats.forEach((file, stats) -> {
         if (stats.readyToBeScheduled(compactionThreshold, stats.getFree())) {
            schedule(file, stats);
         }
      });
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

   // Present for testing only - note is still asynchronous if underlying executor is
   CompletionStage<Void> forceCompactionForAllNonLogFiles() {
      AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      for (Map.Entry<Integer, Stats> stats : fileStats.entrySet()) {
         int fileId = stats.getKey();
         if (!fileProvider.isLogFile(fileId) && !stats.getValue().markedForDeletion && stats.getValue().setScheduled()) {
            CompactionRequest compactionRequest = new CompactionRequest(fileId);
            processor.onNext(compactionRequest);
            aggregateCompletionStage.dependsOn(compactionRequest);
         }
      }
      return aggregateCompletionStage.freeze();
   }

   // Present for testing only - so test can see what files are currently known to compactor
   Set<Integer> getFiles() {
      return fileStats.keySet();
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
         stats.setNextExpirationTime(ExpiryHelper.mostRecentExpirationTime(stats.nextExpirationTime, expirationTime));
      }
      return stats;
   }

   private void recordFreeSpace(Stats stats, int file, int size) {
      if (stats.addFree(size, compactionThreshold)) {
         schedule(file, stats);
      }
   }

   private void schedule(int file, Stats stats) {
      assert stats.isScheduled();
      if (!terminateSignal) {
         log.debugf("Scheduling file %d for compaction: %d/%d free", file, stats.free.get(), stats.total);
         CompactionRequest request = new CompactionRequest(file);
         processor.onNext(request);
         request.whenComplete((__, t) -> {
            if (t != null) {
               log.compactorEncounteredException(t, file);
               // Poor attempt to allow compactor to continue operating - file will never be compacted again
               fileStats.remove(file);
            }
         });
      }
   }

   /**
    * Immediately sends a request to pause the compactor. The returned stage will complete when the
    * compactor is actually paused. To resume the compactor the {@link #resumeAfterClear()} method
    * must be invoked or else the compactor will not process new requests.
    *
    * @return a stage that when complete the compactor is paused
    */
   public CompletionStage<Void> clearAndPause() {
      if (clearSignal.getAndSet(true)) {
         throw new IllegalStateException("Clear signal was already set for compactor, clear cannot be invoked " +
               "concurrently with another!");
      }
      ClearFuture clearFuture = new ClearFuture();
      // Make sure to do this before submitting to processor this is done in the blocking thread
      clearFuture.whenComplete((ignore, t) -> fileStats.clear());
      processor.onNext(clearFuture);
      return clearFuture;
   }

   private static class ClearFuture extends CompletableFuture<Void> {
      @Override
      public String toString() {
         return "ClearFuture{}";
      }
   }

   public void resumeAfterClear() {
      // This completion will push all the other tasks that have been delayed in this method call
      if (!clearSignal.getAndSet(false)) {
         throw new IllegalStateException("Resume of compactor invoked without first clear and pausing!");
      }
   }

   private void resumeAfterPause() {
      processor.onNext(RESUME_PILL);
   }

   public void stopOperations() {
      // This will short circuit any compactor call, so it can only process the entry it may be on currently
      terminateSignal = true;
      processor.onComplete();
      // The stopped CompletableFuture is completed in onComplete or onError callback for the processor, so this will
      // return after all compaction calls are completed
      stopped.join();
      if (logFile != null) {
         Util.close(logFile);
         // Complete the file, this file should not be compacted
         completeFile(logFile.fileId, currentOffset, nextExpirationTime, false);
         logFile = null;
      }

      // Reinitialize processor so it can be started again possibly
      processor = UnicastProcessor.create().toSerialized();
   }

   private static class CompactionRequest extends CompletableFuture<Void> {
      private final int fileId;

      private CompactionRequest(int fileId) {
         this.fileId = fileId;
      }

      @Override
      public String toString() {
         return "CompactionRequest{" +
               "fileId=" + fileId +
               '}';
      }
   }

   void handleIgnoredElement(Object o) {
      if (o instanceof CompactionExpirationSubscriber) {
         // We assume the subscriber handles blocking properly
         ((CompactionExpirationSubscriber) o).onComplete();
      } else if (o instanceof CompletableFuture) {
         nonBlockingManager.complete((CompletableFuture<?>) o, null);
      }
   }

   @Override
   public void accept(Object o) throws Throwable {
      if (terminateSignal) {
         log.tracef("Compactor already terminated, ignoring request " + o);
         // Just ignore if terminated
         handleIgnoredElement(o);
         return;
      }
      if (o == RESUME_PILL) {
         log.tracef("Resuming compactor");
         // This completion will push all the other tasks that have been delayed in this method call
         // Note this must be completed in the context of the compactor thread
         paused.complete(null);
         return;
      }
      // Note that this accept is only invoked from a single thread at a time so we don't have to worry about
      // any other threads decrementing clear signal. However, another thread can increment, that is okay for us
      if (clearSignal.get()) {
         // We ignore any entries since it was last cleared
         if (o instanceof ClearFuture) {
            log.tracef("Compactor ignoring all future compactions until resumed");

            if (logFile != null) {
               logFile.close();
               logFile = null;
               nextExpirationTime = -1;
            }

            nonBlockingManager.complete((CompletableFuture<?>) o, null);
         } else {
            log.tracef("Ignoring compaction request for %s as compactor is being cleared", o);
            handleIgnoredElement(o);
         }
         return;
      }

      if (o instanceof CompactionExpirationSubscriber) {
         CompactionExpirationSubscriber subscriber = (CompactionExpirationSubscriber) o;
         try {
            // We have to copy the file ids into its own collection because it can pickup the compactor files sometimes
            // causing extra unneeded churn in some cases
            Set<Integer> currentFiles = new HashSet<>();
            try (CloseableIterator<Integer> iter = fileProvider.getFileIterator()) {
               while (iter.hasNext()) {
                  currentFiles.add(iter.next());
               }
            }
            for (int fileId : currentFiles) {
               boolean isLogFile = fileProvider.isLogFile(fileId);
               if (isLogFile) {
                  // Force log file to be in the stats
                  free(fileId, 0);
               }
               Stats stats = fileStats.get(fileId);
               long currentTimeMilliseconds = timeService.wallClockTime();
               if (stats != null) {
                  // Don't check for expired entries in any files that are marked for deletion or don't have entries
                  // that can expire yet
                  // Note that log files do not set the expiration time, so it is always -1 in that case, but we still
                  // want to check just in case some files are expired there.
                  // Note that we when compacting an expired entry from the log file we first write to the compacted
                  // file and then notify the subscriber. Assuming the subscriber then invokes remove expired it
                  // will actually cause two writes for the same expired entry. This is required though in case if
                  // the entry is not removed from the listener as we don't want to keep returning the same entry
                  // to the listener that it has expired.
                  if (stats.markedForDeletion() || (!isLogFile && stats.nextExpirationTime == -1) || stats.nextExpirationTime > currentTimeMilliseconds) {
                     log.tracef("Skipping expiration for file %d since it is marked for deletion: %s or its expiration time %s is not yet",
                           (Object) fileId, stats.markedForDeletion(), stats.nextExpirationTime);
                     continue;
                  }
                  // Make sure we don't start another compaction for this file while performing expiration
                  if (stats.setScheduled()) {
                     compactSingleFile(fileId, isLogFile, subscriber, currentTimeMilliseconds);
                     if (isLogFile) {
                        // Unschedule the compaction for log file as we can't remove it
                        stats.scheduled.set(false);
                     }
                  }
               } else {
                  log.tracef("Skipping expiration for file %d as it is not included in fileStats", fileId);
               }
            }
            subscriber.onComplete();
         } catch (Throwable t) {
            subscriber.onError(t);
         }
         return;
      }

      CompactionRequest request = (CompactionRequest) o;
      try {
         // Any other type submitted has to be a positive integer
         Stats stats = fileStats.get(request.fileId);

         // Double check that the file wasn't removed. If stats are null that means the file was previously removed
         // and also make sure the file wasn't marked for deletion, but hasn't yet
         if (stats != null && !stats.markedForDeletion()) {
            compactSingleFile(request.fileId, false, null, timeService.wallClockTime());
         }
         request.complete(null);
      } catch (Throwable t) {
         log.trace("Completing compaction for file: " + request.fileId + " due to exception!", t);
         request.completeExceptionally(t);
      }
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
      if (subscriber == null) {
         log.tracef("Compacting file %d isLogFile %b", scheduledFile, Boolean.valueOf(isLogFile));
      } else {
         log.tracef("Removing expired entries from file %d isLogFile %b", scheduledFile, Boolean.valueOf(isLogFile));
      }
      int scheduledOffset = 0;
      // Store expired entries to remove after we update the index
      List<EntryPosition> expiredTemp = subscriber != null ? new ArrayList<>() : null;
      List<EntryRecord> expiredIndex = subscriber != null ? new ArrayList<>() : null;
      FileProvider.Handle handle = fileProvider.getFile(scheduledFile);
      if (handle == null) {
         throw new IllegalStateException("Compactor should not get deleted file for compaction!");
      }
      try {
         AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
         while (!clearSignal.get() && !terminateSignal) {
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
            // We drop all entries by default unless it is a log file as we can't drop any of those since we may
            // try to compact a log file multiple times, note modifications to drop variable below should only be to set
            // it to false
            int prevFile = -1;
            int prevOffset = -1;
            boolean drop = !isLogFile;
            // Whether to truncate the value
            boolean truncate = false;
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
                        // We can only truncate expired entries if this was compacted with purge expire and this entry
                        // isn't a removed marker
                        if (expiredTemp != null && entry.offset >= 0) {
                           truncate = true;
                           expiredTemp.add(entry);
                        }
                     } else if (isLogFile) {
                        // Non expired entry in a log file, just skip it
                        scheduledOffset += header.totalLength();
                        continue;
                     }
                  } else if (entry.file == scheduledFile && entry.offset == ~scheduledOffset) {
                     // The temporary table doesn't know how many entries we have for a key, so we shouldn't truncate
                     // or drop
                     log.tracef("Key for %d:%d ignored as it was expired but was in temporary table");
                     scheduledOffset += header.totalLength();
                     continue;
                  } else {
                     truncate = true;
                  }
               }
               // When we have found the entry in temporary table, it's possible that the delete operation
               // (that was recorded in temporary table) will arrive to index after DROPPED - in that case
               // we could remove the entry and delete would not find it
               drop = false;
            } else {
               log.tracef("Loading from index for key %s", key);
               EntryInfo info = index.getInfo(key, segment, serializedKey);
               Objects.requireNonNull(info, "No index info found for key: " + key);
               if (info.numRecords <= 0) {
                  throw new IllegalArgumentException("Number of records " + info.numRecords + " for index of key " + key + " should be more than zero!");
               }
               if (info.file == scheduledFile && info.offset == scheduledOffset) {
                  assert header.valueLength() > 0;
                  long entryExpiryTime = header.expiryTime();
                  // live record with data
                  if (entryExpiryTime >= 0 && entryExpiryTime <= currentTimeMilliseconds) {
                     // We can only truncate expired entries if this was compacted with purge expire
                     if (expiredIndex != null) {
                        EntryRecord record = index.getRecordEvenIfExpired(key, segment, serializedKey);
                        truncate = true;
                        expiredIndex.add(record);
                        // If there are more entries we cannot drop the index as we need a tombstone
                        if (info.numRecords > 1) {
                           drop = false;
                        }
                     } else {
                        // We can't drop an expired entry without notifying, so we write it to the new compacted file
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
                     log.tracef("Is key %s at %d:%d expired? %s, numRecords? %d", key, scheduledFile, scheduledOffset, truncate, info.numRecords);
                  }
               } else if (isLogFile) {
                  // If entry doesn't match the index we can't touch it when it is a log file
                  scheduledOffset += header.totalLength();
                  continue;
               } else if (info.file == scheduledFile && info.offset == ~scheduledOffset && info.numRecords > 1) {
                  // The entry was expired, but we have other records so we can't drop this one or else the index will rebuild incorrectly
                  drop = false;
               } else if (log.isTraceEnabled()) {
                  log.tracef("Key %s for %d:%d was found in index on %d:%d, %d record => drop", key,
                        scheduledFile, scheduledOffset, info.file, info.offset, info.numRecords);
               }
               prevFile = info.file;
               prevOffset = info.offset;
            }

            if (drop) {
               if (log.isTraceEnabled()) {
                  log.tracef("Drop index for key %s, file %d:%d (%s)", key, scheduledFile, scheduledOffset,
                        header.valueLength() > 0 ? "record" : "tombstone");
               }
               index.handleRequest(IndexRequest.dropped(segment, key, ByteBufferImpl.create(serializedKey), prevFile, prevOffset, scheduledFile, scheduledOffset));
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
                  // Update the next expiration time only for entries that are not removed
                  nextExpirationTime = ExpiryHelper.mostRecentExpirationTime(nextExpirationTime, header.expiryTime());
               } else {
                  entryOffset = ~currentOffset;
                  writtenLength = header.getHeaderLength() + header.keyLength();
               }
               EntryRecord.writeEntry(logFile.fileChannel, REUSED_BUFFER, serializedKey, metadata, serializedValue, serializedInternalMetadata, header.seqId(), header.expiryTime());
               TemporaryTable.LockedEntry lockedEntry = temporaryTable.replaceOrLock(segment, key, logFile.fileId, entryOffset, scheduledFile, indexedOffset);
               if (lockedEntry == null) {
                  if (log.isTraceEnabled()) {
                     log.trace("Found entry in temporary table");
                  }
               } else {
                  boolean update = false;
                  try {
                     EntryInfo info = index.getInfo(key, segment, serializedKey);
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
               ByteBuffer keyBuffer = ByteBufferImpl.create(serializedKey);
               if (isLogFile) {
                  // When it is a log file we are still keeping the original entry, we are just updating it to say
                  // it was expired
                  indexRequest = IndexRequest.update(segment, key, keyBuffer, false, logFile.fileId, entryOffset, writtenLength);
               } else {
                  // entryFile cannot be used as we have to report the file due to free space statistics
                  indexRequest = IndexRequest.moved(segment, key, keyBuffer, logFile.fileId, entryOffset, writtenLength,
                        scheduledFile, indexedOffset);
               }
               aggregateCompletionStage.dependsOn(index.handleRequest(indexRequest));

               currentOffset += writtenLength;
            }
            scheduledOffset += header.totalLength();
         }
         if (!clearSignal.get()) {
            // We delay the next operation until all prior moves are done. By moving it can trigger another
            // compaction before the index has been fully updated. Thus we block any other compaction events
            // until all entries have been moved for this file
            CompletionStage<Void> aggregate = aggregateCompletionStage.freeze();
            if (!CompletionStages.isCompletedSuccessfully(aggregate)) {
               paused = new CompletableFuture<>();
               // We resume after completed, Note that we must complete the {@code paused} variable inside the compactor
               // execution pipeline otherwise we can invoke compactor operations in the wrong thread
               aggregate.whenComplete((ignore, t) -> {
                  resumeAfterPause();
                  if (t != null) {
                     log.error("There was a problem moving indexes for compactor with file " + logFile.fileId, t);
                  }
               });
            }
         }
      } finally {
         handle.close();
      }
      if (subscriber != null) {
         for (EntryPosition entryPosition : expiredTemp) {
            subscriber.onEntryPosition(entryPosition);
         }
         for (EntryRecord entryRecord : expiredIndex) {
            subscriber.onEntryEntryRecord(entryRecord);
         }
      }
      if (isLogFile) {
         log.tracef("Finished expiring entries in log file %d, leaving file as is", scheduledFile);
      } else if (!terminateSignal && !clearSignal.get()) {
         // The deletion must be executed only after the index is fully updated.
         log.tracef("Finished compacting %d, scheduling delete", scheduledFile);
         // Mark the file for deletion so expiration won't check it
         Stats stats = fileStats.get(scheduledFile);
         if (stats != null) {
            stats.markForDeletion();
         }
         index.deleteFileAsync(scheduledFile);
      } else {
         log.tracef("Not doing anything to compacted file %d as either the terminate clear signal were set", scheduledFile);
      }
   }


   static class Stats {
      private final AtomicInteger free;
      private volatile int total;
      private volatile long nextExpirationTime;
      /* File is not 'completed' when we have not loaded that yet completely.
         Files created by log appender/compactor are completed as soon as it closes them.
         File cannot be scheduled for compaction until it's completed.
         */
      private volatile boolean completed = false;
      private final AtomicBoolean scheduled = new AtomicBoolean();
      private boolean markedForDeletion = false;

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

      public long getNextExpirationTime() {
         return nextExpirationTime;
      }

      public void setNextExpirationTime(long nextExpirationTime) {
         this.nextExpirationTime = nextExpirationTime;
      }

      public boolean readyToBeScheduled(double compactionThreshold, int free) {
         int total = this.total;
         // Note setScheduled must be last as it changes state
         return completed && total >= 0 && free >= total * compactionThreshold && setScheduled();
      }

      public boolean isScheduled() {
         return scheduled.get();
      }

      public boolean setScheduled() {
         return !scheduled.getAndSet(true);
      }

      public boolean isCompleted() {
         return completed;
      }

      public void setCompleted() {
         this.completed = true;
      }

      public void markForDeletion() {
         this.markedForDeletion = true;
      }

      public boolean markedForDeletion() {
         return this.markedForDeletion;
      }

      @Override
      public String toString() {
         return "Stats{" +
               "free=" + free +
               ", total=" + total +
               ", nextExpirationTime=" + nextExpirationTime +
               ", completed=" + completed +
               ", scheduled=" + scheduled +
               ", markedForDeletion=" + markedForDeletion +
               '}';
      }
   }
}
