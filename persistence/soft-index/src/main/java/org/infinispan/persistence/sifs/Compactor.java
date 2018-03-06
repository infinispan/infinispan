package org.infinispan.persistence.sifs;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.LogFactory;

/**
 * Component keeping the data about log file usage - as soon as entries from some file are overwritten so that the file
 * becomes cluttered with old records, the valid records are moved to another file and the old ones are dropped.
 * Expired records are moved as tombstones without values (records of entry removal).
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class Compactor extends Thread {
   private static final Log log = LogFactory.getLog(Compactor.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ConcurrentMap<Integer, Stats> fileStats = new ConcurrentHashMap<Integer, Stats>();
   private final BlockingQueue<Integer> scheduledCompaction = new LinkedBlockingQueue<Integer>();
   private final BlockingQueue<IndexRequest> indexQueue;
   private final FileProvider fileProvider;
   private final TemporaryTable temporaryTable;
   private final Marshaller marshaller;
   private final TimeService timeService;
   private final int maxFileSize;
   private final double compactionThreshold;

   private Index index;
   // as processing single scheduled compaction takes a lot of time, we don't use the queue to signalize
   private volatile boolean clearSignal = false;
   private volatile boolean terminateSignal = false;
   private volatile CountDownLatch compactorResume;
   private volatile CountDownLatch compactorStop;


   public Compactor(FileProvider fileProvider,
                    TemporaryTable temporaryTable,
                    BlockingQueue<IndexRequest> indexQueue,
                    Marshaller marshaller, TimeService timeService, int maxFileSize, double compactionThreshold) {
      super("BCS-Compactor");
      this.fileProvider = fileProvider;
      this.temporaryTable = temporaryTable;
      this.indexQueue = indexQueue;
      this.marshaller = marshaller;
      this.timeService = timeService;
      this.maxFileSize = maxFileSize;
      this.compactionThreshold = compactionThreshold;
      this.start();
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
      try {
         synchronized (stats) {
            if (!stats.isScheduled()) {
               log.debug(String.format("Scheduling file %d for compaction: %d/%d free", file, stats.free.get(), stats.total));
               stats.setScheduled();
               scheduledCompaction.put(file);
            }
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new RuntimeException(e);
      }
   }

   @Override
   public void run() {
      try {
         FileProvider.Log logFile = null;
         int currentOffset = 0;
         for(;;) {
            Integer scheduledFile = null;
            try {
               scheduledFile = scheduledCompaction.poll(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
            }
            if (terminateSignal) {
               if (logFile != null) {
                  logFile.close();
                  completeFile(logFile.fileId);
               }
               break;
            }
            if (clearSignal) {
               pauseCompactor(logFile);
               logFile = null;
               continue;
            }
            if (scheduledFile == null) {
               if (logFile != null) {
                  logFile.close();
                  completeFile(logFile.fileId);
                  logFile = null;
               }
               continue;
            }

            log.debugf("Compacting file %d", scheduledFile);
            int scheduledOffset = 0;
            FileProvider.Handle handle = fileProvider.getFile(scheduledFile);
            if (handle == null) {
               throw new IllegalStateException("Compactor should not get deleted file for compaction!");
            }
            try {
               while (!clearSignal && !terminateSignal) {
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

                  int indexedOffset = header.valueLength() > 0 ? scheduledOffset : ~scheduledOffset;
                  boolean drop = true;
                  boolean truncate = false;
                  EntryPosition entry = temporaryTable.get(key);
                  if (entry != null) {
                     synchronized (entry) {
                        if (trace) {
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
                        if (trace) {
                           log.tracef("Is %d:%d expired? %s, numRecords? %d", scheduledFile, scheduledOffset, truncate, info.numRecords);
                        }
                        if (!truncate || info.numRecords > 1) {
                           drop = false;
                        }
                        // Drop only when it is expired and has single record
                     } else if (info.file == scheduledFile && info.offset == ~scheduledOffset && info.numRecords > 1) {
                        // just tombstone but there are more non-compacted records for this key so we have to keep it
                        drop = false;
                     } else if (trace) {
                        log.tracef("Key for %d:%d was found in index on %d:%d, %d record => drop",
                              scheduledFile, scheduledOffset, info.file, info.offset, info.numRecords);
                     }
                  }
                  if (drop) {
                     if (trace) {
                        log.tracef("Drop %d:%d (%s)", scheduledFile, (Object)scheduledOffset,
                              header.valueLength() > 0 ? "record" : "tombstone");
                     }
                     indexQueue.put(IndexRequest.dropped(key, serializedKey, scheduledFile, scheduledOffset));
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
                     byte[] serializedMetadata = null;
                     int entryOffset;
                     int writtenLength;
                     if (header.valueLength() > 0 && !truncate) {
                        if (header.metadataLength() > 0) {
                           serializedMetadata = EntryRecord.readMetadata(handle, header, scheduledOffset);
                        }
                        serializedValue = EntryRecord.readValue(handle, header, scheduledOffset);
                        entryOffset = currentOffset;
                        writtenLength = header.totalLength();
                     } else {
                        entryOffset = ~currentOffset;
                        writtenLength = EntryHeader.HEADER_SIZE + header.keyLength();
                     }
                     EntryRecord.writeEntry(logFile.fileChannel, serializedKey, serializedMetadata, serializedValue, header.seqId(), header.expiryTime());
                     TemporaryTable.LockedEntry lockedEntry = temporaryTable.replaceOrLock(key, logFile.fileId, entryOffset, scheduledFile, indexedOffset);
                     if (lockedEntry == null) {
                        if (trace) {
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
                           if (trace) {
                              log.tracef("In index the key is on %d:%d (%s)", info.file, info.offset, String.valueOf(update));
                           }
                        } finally {
                           if (update) {
                              temporaryTable.updateAndUnlock(lockedEntry, logFile.fileId, entryOffset);
                           } else {
                              temporaryTable.removeAndUnlock(lockedEntry, key);
                           }
                        }
                     }
                     if (trace) {
                        log.tracef("Update %d:%d -> %d:%d | %d,%d", scheduledFile, indexedOffset,
                              logFile.fileId, entryOffset, logFile.fileChannel.position(), logFile.fileChannel.size());
                     }
                     // entryFile cannot be used as we have to report the file due to free space statistics
                     indexQueue.put(IndexRequest.moved(key, serializedKey, logFile.fileId, entryOffset, writtenLength,
                           scheduledFile, indexedOffset));

                     currentOffset += writtenLength;
                  }
                  scheduledOffset += header.totalLength();
               }
            } finally {
               handle.close();
            }
            if (terminateSignal) {
               if (logFile != null) {
                  logFile.close();
                  completeFile(logFile.fileId);
               }
               return;
            } else if (clearSignal) {
               pauseCompactor(logFile);
               logFile = null;
            } else {
               // The deletion must be executed only after the index is fully updated.
               log.debugf("Finished compacting %d, scheduling delete", scheduledFile);
               indexQueue.put(IndexRequest.deleteFileRequest(scheduledFile));
            }
         }
      } catch (Exception e) {
         log.compactorFailed(e);
      }
   }

   private void pauseCompactor(FileProvider.Log logFile) throws IOException, InterruptedException {
      if (logFile != null) {
         logFile.close();
         completeFile(logFile.fileId);
      }
      compactorStop.countDown();
      compactorResume.await();
   }

   public void clearAndPause() throws InterruptedException {
      compactorResume = new CountDownLatch(1);
      compactorStop = new CountDownLatch(1);
      clearSignal = true;
      scheduledCompaction.put(-1);
      compactorStop.await();
      scheduledCompaction.clear();
      fileStats.clear();
   }

   public void resumeAfterPause() {
      clearSignal = false;
      compactorResume.countDown();
   }

   public void stopOperations() throws InterruptedException {
      terminateSignal = true;
      scheduledCompaction.put(-1);
      this.join();
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
