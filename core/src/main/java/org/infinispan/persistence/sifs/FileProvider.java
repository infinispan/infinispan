package org.infinispan.persistence.sifs;

import static org.infinispan.util.logging.Log.PERSISTENCE;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.persistence.sifs.pmem.PmemUtilWrapper;
import org.infinispan.util.logging.LogFactory;

/**
 * Provides resource management for files - only limited amount of files may be opened in one moment, and opened file
 * should not be deleted. Also allows to generate file indexes.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class FileProvider {
   private static final org.infinispan.persistence.sifs.Log log =
         LogFactory.getLog(FileProvider.class, org.infinispan.persistence.sifs.Log.class);

   private static final String REGEX_FORMAT = "^%s[0-9]+$";
   private static final boolean ATTEMPT_PMEM;

   private final File directoryFile;
   private final int openFileLimit;
   private final ArrayBlockingQueue<Record> recordQueue;
   private final ConcurrentMap<Integer, Record> openFiles = new ConcurrentHashMap<>();
   private final AtomicInteger currentOpenFiles = new AtomicInteger(0);
   private final ReadWriteLock lock = new ReentrantReadWriteLock();
   private final Set<Integer> logFiles = new HashSet<>();
   private final Set<FileIterator> iterators = ConcurrentHashMap.newKeySet();
   private final String prefix;
   private final int maxFileSize;

   private boolean canTryPmem = true;

   private int nextFileId = 0;

   static {
      boolean attemptPmem = false;
      try {
         Class.forName("io.mashona.logwriting.PmemUtil");
         // use persistent memory if available, otherwise fallback to regular file.
         attemptPmem = true;
      } catch (ClassNotFoundException e) {
         log.debug("Persistent Memory not in classpath, not attempting");
      }
      ATTEMPT_PMEM = attemptPmem;
   }

   public FileProvider(Path fileDirectory, int openFileLimit, String prefix, int maxFileSize) {
      this.openFileLimit = openFileLimit;
      this.recordQueue = new ArrayBlockingQueue<>(openFileLimit);
      this.directoryFile = fileDirectory.toFile();
      this.prefix = prefix;
      this.maxFileSize = maxFileSize;
      try {
         Files.createDirectories(fileDirectory);
      } catch (IOException e) {
         throw PERSISTENCE.directoryCannotBeCreated(this.directoryFile.getAbsolutePath());
      }
   }

   public boolean isLogFile(int fileId) {
      lock.readLock().lock();
      try {
         return logFiles.contains(fileId);
      } finally {
         lock.readLock().unlock();
      }
   }

   public Handle getFileIfOpen(int fileId) {
      lock.readLock().lock();
      try {
         Record record = openFiles.get(fileId);
         if (record != null) {
            synchronized (record) {
               if (record.isOpen()) {
                  return new Handle(record);
               }
            }
         }
         return null;
      } finally {
         lock.readLock().unlock();
      }
   }

   public Handle getFile(int fileId) throws IOException {
      lock.readLock().lock();
      try {
         for (; ; ) {
            Record record = openFiles.get(fileId);
            if (record == null) {
               for (; ; ) {
                  int open = currentOpenFiles.get();
                  if (open >= openFileLimit) {
                     // we'll continue only after some other file will be closed
                     if (tryCloseFile()) break;
                  } else {
                     if (currentOpenFiles.compareAndSet(open, open + 1)) {
                        break;
                     }
                  }
                  Thread.yield();
               }
               // now we have either removed some other opened file or incremented the value below limit
               for (;;) {
                  FileChannel fileChannel;
                  try {
                     fileChannel = openChannel(fileId);
                  } catch (FileNotFoundException e) {
                     currentOpenFiles.decrementAndGet();
                     log.debugf(e, "File %d was not found", fileId);
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
         return newFile(file).length();
      } finally {
         lock.readLock().unlock();
      }
   }

   private String fileIdToString(int fileId) {
      return prefix + fileId;
   }

   // Package private for tests
   File newFile(int fileId) {
      return new File(directoryFile, fileIdToString(fileId));
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
      return openChannel(newFile(fileId), false, true);
   }

   protected FileChannel openChannel(File file, boolean create, boolean readSharedMeadata) throws FileNotFoundException {
      log.debugf("openChannel(%s)", file.getAbsolutePath());

      FileChannel fileChannel = ATTEMPT_PMEM && canTryPmem ? PmemUtilWrapper.pmemChannelFor(file, maxFileSize, create, readSharedMeadata) : null;

      if (fileChannel == null) {
         canTryPmem = false;
         if (create) {
            fileChannel = new FileOutputStream(file).getChannel();
         } else {
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
         }
      }

      return fileChannel;
   }

   public Log getFileForLog() throws IOException {
      lock.writeLock().lock();
      try {
         for (;;) {
            File f = newFile(nextFileId);
            if (f.exists()) {
               if (nextFileId == Integer.MAX_VALUE) {
                  nextFileId = 0;
               } else {
                  nextFileId++;
               }
            } else {
               logFiles.add(nextFileId);
               for (FileIterator it : iterators) {
                  it.add(nextFileId);
               }

               // use persistent memory if available, otherwise fallback to regular file.
               FileChannel fileChannel = openChannel(f, true, false);
               if (fileChannel == null) {
                  fileChannel = new FileOutputStream(f).getChannel();
               }
               return new Log(nextFileId, fileChannel);
            }
         }
      } finally {
         lock.writeLock().unlock();
      }
   }

   public CloseableIterator<Integer> getFileIterator() {
      String regex = String.format(REGEX_FORMAT, prefix);
      lock.readLock().lock();
      try {
         Set<Integer> set = new HashSet<>();
         for (String file : directoryFile.list()) {
            if (file.matches(regex)) {
               set.add(Integer.parseInt(file.substring(prefix.length())));
            }
         }
         FileIterator iterator = new FileIterator(set.iterator());
         iterators.add(iterator);
         return iterator;
      } finally {
         lock.readLock().unlock();
      }
   }

   public boolean hasFiles() {
      String regex = String.format(REGEX_FORMAT, prefix);
      lock.readLock().lock();
      try {
         for (String file : directoryFile.list()) {
            if (file.matches(regex)) {
               return true;
            }
         }
         return false;
      } finally {
         lock.readLock().unlock();
      }
   }

   public void clear() throws IOException {
      lock.writeLock().lock();
      try {
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
         File[] files = directoryFile.listFiles();
         if (files != null) {
            for (File file : files) {
               Files.delete(file.toPath());
            }
         }
      } finally {
         lock.writeLock().unlock();
      }
   }

   public void deleteFile(int fileId) {
      lock.readLock().lock();
      try {
         for (;;) {
            Record newRecord = new Record(null, fileId);
            Record record = openFiles.putIfAbsent(fileId, newRecord);
            if (record == null) {
               try {
                  newRecord.delete();
               } catch (IOException e) {
                  log.cannotCloseDeleteFile(fileId, e);
               }
               openFiles.remove(fileId, newRecord);
               return;
            }
            synchronized (record) {
               if (openFiles.get(fileId) == record) {
                  try {
                     record.deleteOnClose();
                  } catch (IOException e) {
                     log.cannotCloseDeleteFile(fileId, e);
                  }
                  break;
               }
            }
         }
      } finally {
         lock.readLock().unlock();
      }
   }

   public void stop() {
      int open = currentOpenFiles.get();
      while (open > 0) {
         try {
            if (tryCloseFile()) {
               open = currentOpenFiles.decrementAndGet();
            } else {
               // we can't close any further file
               break;
            }
         } catch (IOException e) {
            log.cannotCloseFile(e);
         }
      }
      if (currentOpenFiles.get() != 0) {
         for (Map.Entry<Integer, Record> entry : openFiles.entrySet()) {
            log.debugf("File %d has %d open handles", entry.getKey().intValue(), entry.getValue().handleCount);
         }
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
      private final Record record;

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

      public void truncate(long i) throws IOException {
         if (!usable) throw new IllegalStateException();
         record.getFileChannel().truncate(i);
      }

      public int write(ByteBuffer buffer, long l) throws IOException {
         if (!usable) throw new IllegalStateException();
         return record.getFileChannel().write(buffer, l);
      }

      public void force(boolean metaData) throws IOException {
         if (!usable) throw new IllegalStateException();
         record.getFileChannel().force(metaData);
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

      public void delete() throws IOException {
         log.debugf("Deleting file %s", fileIdToString(fileId));
         //noinspection ResultOfMethodCallIgnored
         Files.deleteIfExists(newFile(fileId).toPath());
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

   private class FileIterator implements CloseableIterator<Integer> {
      private final Iterator<Integer> diskFiles;
      private final ConcurrentLinkedQueue<Integer> addedFiles = new ConcurrentLinkedQueue<>();

      private FileIterator(Iterator<Integer> diskFiles) {
         this.diskFiles = diskFiles;
      }

      public void add(int file) {
         addedFiles.add(file);
      }

      @Override
      public void close() {
         iterators.remove(this);
      }

      @Override
      public boolean hasNext() {
         return diskFiles.hasNext() || !addedFiles.isEmpty();
      }

      @Override
      public Integer next() {
         return diskFiles.hasNext() ? diskFiles.next() : addedFiles.poll();
      }
   }
}
