package org.infinispan.loaders.bcs;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Component keeping the data about log file usage - as soon as entries from some file are overwritten so that the file
 * becomes cluttered with old records, the valid records are moved to another file and the old ones are dropped.
 * Expired records are moved as tombstones without values (records of entry removal).
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class Compactor extends Thread {
   private static final Log log = LogFactory.getLog(Compactor.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ConcurrentMap<Integer, Stats> fileStats = new ConcurrentHashMap<Integer, Stats>();
   private final BlockingQueue<Integer> scheduledCompaction = new LinkedBlockingQueue<Integer>();
   private final BlockingQueue<IndexRequest> indexQueue;
   private final FileProvider fileProvider;
   private final TemporaryTable temporaryTable;
   private final Marshaller marshaller;
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
                    Marshaller marshaller, int maxFileSize, double compactionThreshold) {
      super("BCS-Compactor");
      this.fileProvider = fileProvider;
      this.temporaryTable = temporaryTable;
      this.marshaller = marshaller;
      this.maxFileSize = maxFileSize;
      this.indexQueue = indexQueue;
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
      Stats stats = fileStats.get(file);
      if (stats == null) {
         int fileSize = (int) fileProvider.getFileSize(file);
         stats = new Stats(fileSize, size);
         Stats other = fileStats.putIfAbsent(file, stats);
         if (other != null) {
            if (fileSize > other.getTotal()) {
               other.setTotal(fileSize);
            }
            recordFreeSpace(other, file, size);
            return;
         }
      }
      if (stats.getTotal() < 0) {
         int fileSize = (int) fileProvider.getFileSize(file);
         if (fileSize >= 0) {
            stats.setTotal(fileSize);
         }
      }
      recordFreeSpace(stats, file, size);
   }

   private void recordFreeSpace(Stats stats, int file, int size) {
      if (stats.addFree(size, compactionThreshold) && !stats.isScheduled()) {
         try {
            synchronized (stats) {
               if (!stats.isScheduled()) {
                  log.debug(String.format("Scheduling file %d for compaction: %d/%d free", file, stats.free.get(), stats.total));
                  stats.setScheduled();
                  scheduledCompaction.put(file);
               }
            }
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
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
                  logFile = null;
               }
               continue;
            }

            log.debug("Compacting file " + scheduledFile);
            int scheduledOffset = 0;
            FileProvider.Handle handle = fileProvider.getFile(scheduledFile);
            if (handle == null) {
               throw new IllegalStateException("Compactor should not get deleted file for compaction!");
            }
            try {
               long currentTime = System.currentTimeMillis();
               while (!clearSignal && !terminateSignal) {
                  EntryReaderWriter.EntryHeader header = EntryReaderWriter.readEntryHeader(handle, scheduledOffset);
                  if (header == null) {
                     break;
                  }
                  byte[] serializedKey = EntryReaderWriter.readKey(handle, header, scheduledOffset);
                  Object key = marshaller.objectFromByteBuffer(serializedKey);

                  boolean drop = true;
                  EntryPosition entry = temporaryTable.get(key);
                  if (entry != null) {
                     synchronized (entry) {
                        if (entry.file == scheduledFile && entry.offset == scheduledOffset) {
                           drop = false;
                        } else if (trace) {
                           log.trace(String.format("Key for %d:%d was found in temporary table on %d:%d",
                                 scheduledFile, scheduledOffset, entry.file, entry.offset));
                        }
                     }
                  } else {
                     EntryPosition position = index.getPosition(key, serializedKey);
                     if (position != null && position.file == scheduledFile && position.offset == scheduledOffset) {
                        drop = false;
                     } else if (trace) {
                        if (position != null) {
                           log.trace(String.format("Key for %d:%d was found in index on %d:%d",
                                 scheduledFile, scheduledOffset, position.file, position.offset));
                        } else {
                           log.trace(String.format("Key for %d:%d was not found in index!", scheduledFile, scheduledOffset));
                        }
                     }
                  }
                  if (drop) {
                     if (trace) {
                        log.trace(String.format("Drop %d:%d (%s)", scheduledFile, scheduledOffset,
                              header.valueLength() > 0 ? "record" : "tombstone"));
                     }
                     scheduledOffset += header.totalLength();
                     continue;
                  }

                  if (logFile == null || currentOffset + header.totalLength() > maxFileSize) {
                     if (logFile != null) {
                        logFile.close();
                     }
                     currentOffset = 0;
                     logFile = fileProvider.getFileForLog();
                     log.debug("Compacting to " + logFile.fileId);
                  }

                  byte[] serializedValue;
                  int entryOffset;
                  // we have to keep track of expired entries but only as tombstones - do not copy the value
                  if (header.valueLength() > 0 && (header.expiryTime() < 0 || header.expiryTime() > currentTime)) {
                     serializedValue = EntryReaderWriter.readValue(handle, header, scheduledOffset);
                     entryOffset = currentOffset;
                  } else {
                     serializedValue = null;
                     entryOffset = ~currentOffset;
                  }

                  EntryReaderWriter.writeEntry(logFile.fileChannel, serializedKey, serializedValue, header.seqId(), header.expiryTime());
                  temporaryTable.setConditionally(key, logFile.fileId, entryOffset, scheduledFile, scheduledOffset);
                  if (trace) {
                     log.trace(String.format("Update %d:%d -> %d:%d | %d,%d", scheduledFile, scheduledOffset, logFile.fileId, entryOffset, logFile.fileChannel.position(), logFile.fileChannel.size()));
                  }
                  // entryFile cannot be used as we have to report the file due to free space statistics
                  indexQueue.put(new IndexRequest(key, serializedKey,
                        logFile.fileId, entryOffset,
                        header.totalLength(), scheduledFile, scheduledOffset));

                  currentOffset += header.totalLength();
                  scheduledOffset += header.totalLength();
               }
            } finally {
               handle.close();
            }
            if (terminateSignal) {
               if (logFile != null) {
                  logFile.close();
               }
               return;
            } else if (clearSignal) {
               pauseCompactor(logFile);
               logFile = null;
            } else {
               // The deletion must be executed only after the index is fully updated.
               indexQueue.put(IndexRequest.deleteFileRequest(scheduledFile));
            }
         }
      } catch (IOException e) {
         throw new RuntimeException(e);
      } catch (ClassNotFoundException e) {
         throw new RuntimeException(e);
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   private void pauseCompactor(FileProvider.Log logFile) throws IOException, InterruptedException {
      if (logFile != null) {
         logFile.close();
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
         int total = this.total;
         return total >= 0 && free > total * compactionThreshold;
      }

      public boolean isScheduled() {
         return scheduled;
      }

      public void setScheduled() {
         scheduled = true;
      }
   }
}
