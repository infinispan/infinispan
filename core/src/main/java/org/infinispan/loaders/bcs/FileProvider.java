package org.infinispan.loaders.bcs;

import org.infinispan.util.logging.LogFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Provides resource management for files - only limited amount of files may be opened in one moment, and opened file
 * should not be deleted. Also allows to generate file indexes.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class FileProvider {
   private static final org.infinispan.util.logging.Log log = LogFactory.getLog(FileProvider.class);

   private final File dataDir;
   private final int openFileLimit;
   private final ArrayBlockingQueue<Record> recordQueue;
   private final ConcurrentMap<Integer, Record> openFiles = new ConcurrentHashMap<Integer, Record>();
   private final AtomicInteger currentOpenFiles = new AtomicInteger(0);
   private final ReadWriteLock lock = new ReentrantReadWriteLock();
   private final Set<Integer> logFiles = new HashSet<Integer>();

   private int nextFileId = 0;

   public FileProvider(String dataDir, int openFileLimit) {
      this.openFileLimit = openFileLimit;
      this.recordQueue = new ArrayBlockingQueue<Record>(openFileLimit);
      this.dataDir = new File(dataDir);
      this.dataDir.mkdirs();
   }

   public Handle getFile(int fileId) throws IOException {
      lock.readLock().lock();
      try {
         for (;;) {
            Record record = openFiles.get(fileId);
            if (record == null) {
               for (;;) {
                  int open = currentOpenFiles.get();
                  if (open >= openFileLimit) {
                     // we'll continue only after some other file will be closed
                     if (tryCloseFile()) break;
                  } else {
                     if (currentOpenFiles.compareAndSet(open, open + 1)) {
                        break;
                     }
                  }
               }
               // now we have either removed some other opened file or incremented the value below limit
               for (;;) {
                  FileChannel fileChannel;
                  try {
                     fileChannel = openChannel(fileId);
                  } catch (FileNotFoundException e) {
                     currentOpenFiles.decrementAndGet();
                     return null;
                  }
                  Record newRecord = new Record(fileChannel, fileId);
                  Record other = openFiles.putIfAbsent(fileId, newRecord);
                  if (other != null) {
                     fileChannel.close();
                     synchronized (other) {
                        if (other.isOpen()) {
                           // we have allocated opening a new file but then we use an old one
                           currentOpenFiles.decrementAndGet();
                           return new Handle(other);
                        }
                     }
                  } else {
                     Handle handle;
                     synchronized (newRecord) {
                        // the new file cannot be closed but it can be simultaneously fetched multiple times
                        if (!newRecord.isOpen()) {
                           throw new IllegalStateException();
                        }
                        handle = new Handle(newRecord);
                     }
                     try {
                        recordQueue.put(newRecord);
                     } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                     }
                     return handle;
                  }
               }
            }
            synchronized (record) {
               if (record.isOpen()) {
                  return new Handle(record);
               }
            }
         }
      } finally {
         lock.readLock().unlock();
      }
   }

   public long getFileSize(int file) {
      lock.readLock().lock();
      try {
         if (logFiles.contains(file)) {
            return -1;
         }
         return new File(dataDir, String.valueOf(file)).length();
      } finally {
         lock.readLock().unlock();
      }
   }

   private boolean tryCloseFile() throws IOException {
      Record removed;
      try {
         removed = recordQueue.take();
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
      synchronized (removed) {
         if (removed.isUsed()) {
            try {
               recordQueue.put(removed);
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
         } else {
            if (removed.isOpen()) {
               // if the file was marked deleteOnClose it may have been already closed, but it couldn't be removed from
               // the queue
               removed.close();
               openFiles.remove(removed.getFileId(), removed);
            }
            return true;
         }
      }
      return false;
   }

   protected FileChannel openChannel(int fileId) throws FileNotFoundException {
      return new RandomAccessFile(new File(dataDir, String.valueOf(fileId)), "r").getChannel();
   }

   public Log getFileForLog() throws IOException {
      lock.writeLock().lock();
      try {
         for (;;) {
            File f = new File(dataDir, String.valueOf(nextFileId));
            if (f.exists()) {
               if (nextFileId == Integer.MAX_VALUE) {
                  nextFileId = 0;
               } else {
                  nextFileId++;
               }
            } else {
               logFiles.add(nextFileId);
               return new Log(nextFileId, new FileOutputStream(new File(dataDir, String.valueOf(nextFileId))).getChannel());
            }
         }
      } finally {
         lock.writeLock().unlock();
      }
   }

   public Iterator<Integer> getFileIterator() {
      Set<Integer> set = new HashSet<Integer>();
      for (String file : dataDir.list()) {
         if (file.matches("[0-9]*")) {
            set.add(Integer.parseInt(file));
         }
      }
      return set.iterator();
   }

   public void clear() throws IOException {
      lock.writeLock().lock();
      log.debug("Dropping all data");
      while (currentOpenFiles.get() > 0) {
         if (tryCloseFile()) {
            if (currentOpenFiles.decrementAndGet() == 0) {
               break;
            }
         }
      }
      if (!recordQueue.isEmpty()) throw new IllegalStateException();
      if (!openFiles.isEmpty()) throw new IllegalStateException();
      for (File file : dataDir.listFiles()) {
         if (!file.delete()) {
            throw new IOException("Cannot delete file " + file);
         }
      }
      lock.writeLock().unlock();
   }

   public void deleteFile(int fileId) {
      lock.readLock().lock();
      try {
         for (;;) {
            Record newRecord = new Record(null, fileId);
            Record record = openFiles.putIfAbsent(fileId, newRecord);
            if (record == null) {
               newRecord.delete();
               openFiles.remove(fileId, newRecord);
               return;
            }
            synchronized (record) {
               if (openFiles.get(fileId) == record) {
                  try {
                     record.deleteOnClose();
                  } catch (IOException e) {
                     log.error("Cannot close/delete file " + fileId, e);
                  }
                  break;
               }
            }
         }
      } finally {
         lock.readLock().unlock();
      }
   }

   public final class Log implements Closeable {
      public final int fileId;
      public final FileChannel fileChannel;

      public Log(int fileId, FileChannel fileChannel) {
         this.fileId = fileId;
         this.fileChannel = fileChannel;
      }

      @Override
      public void close() throws IOException {
         fileChannel.close();
         lock.writeLock().lock();
         try {
            logFiles.remove(fileId);
         } finally {
            lock.writeLock().unlock();
         }
      }
   }

   public static final class Handle implements Closeable {
      private boolean usable = true;
      private Record record;

      private Handle(Record record) {
         this.record = record;
         record.increaseHandleCount();
      }

      public int read(ByteBuffer buffer, long offset) throws IOException {
         if (!usable) throw new IllegalStateException();
         return record.getFileChannel().read(buffer, offset);
      }

      @Override
      public void close() throws IOException {
         usable = false;
         synchronized (record) {
            record.decreaseHandleCount();
         }
      }

      public long getFileSize() throws IOException {
         return record.fileChannel.size();
      }

      public int getFileId() {
         return record.getFileId();
      }
   }

   private class Record {
      private final int fileId;
      private FileChannel fileChannel;
      private int handleCount;
      private boolean deleteOnClose = false;

      private Record(FileChannel fileChannel, int fileId) {
         this.fileChannel = fileChannel;
         this.fileId = fileId;
      }

      FileChannel getFileChannel() {
         return fileChannel;
      }

      void increaseHandleCount() {
         handleCount++;
      }

      void decreaseHandleCount() throws IOException {
         handleCount--;
         if (handleCount == 0 && deleteOnClose) {
            // we cannot easily remove the record from queue - keep it there until collection,
            // but physically close and delete the file
            fileChannel.close();
            fileChannel = null;
            openFiles.remove(fileId, this);
            delete();
         }
      }

      boolean isOpen() {
         return fileChannel != null;
      }

      boolean isUsed() {
         return handleCount > 0;
      }

      public int getFileId() {
         return fileId;
      }

      public void close() throws IOException {
         fileChannel.close();
         fileChannel = null;
         if (deleteOnClose) {
            delete();
         }
      }

      public void delete() {
         log.debug("Deleting file " + fileId);
         new File(dataDir, String.valueOf(fileId)).delete();
      }

      public void deleteOnClose() throws IOException {
         if (handleCount == 0) {
            if (fileChannel != null) {
               fileChannel.close();
               fileChannel = null;
            }
            openFiles.remove(fileId, this);
            delete();
         } else {
            log.debug("Marking file " + fileId + " for deletion");
            deleteOnClose = true;
         }
      }
   }
}
