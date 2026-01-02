package org.infinispan.commons.util.concurrent;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;

import org.infinispan.commons.util.Util;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * A simplified global lock backed by the file system.
 * <p>
 * This implementation allows to control the access to external resource between different virtual machines. A file
 * is created to control access, where subsequent tries do not succeed acquiring the lock as long as the file exists.
 * </p>
 * <p>
 * <b>Warning:</b> This implementation <b>does not</b> have the semantics of a traditional {@link java.util.concurrent.locks.Lock}.
 * Does <b>not</b> provide memory or visibility guarantees following the JMM.
 * </p>
 *
 * @since 15.0
 */
@ThreadSafe
public class FileSystemLock {

   private final Path directory;
   private final String name;

   @GuardedBy("this")
   private FileOutputStream globalLockFile;

   @GuardedBy("this")
   private FileLock globalLock;

   /**
    * Creates new instance of the lock.
    * <p>
    * Creates a new file in the provided directory utilizing the lock's name.
    * </p>
    *
    * @param directory: Root directory to create the lock files.
    * @param name: Uniquely identify the file name. If the names conflict, it means the lock is already held.
    */
   public FileSystemLock(Path directory, String name) {
      this.directory = directory;
      this.name = name;
   }

   /**
    * Tries to acquire the global lock.
    * <p>
    * Creates a file in the provided directory with the lock's name. If the file already exists, the lock is held
    * by another instance. The instance is not necessarily in the same virtual machine.
    * </p>
    *
    * @return <code>true</code> if acquired the lock, <code>false</code>, otherwise.
    * @throws IOException In case of failures creating the file.
    * @see FileChannel#lock() Check the thrown exceptions.
    */
   public synchronized boolean tryLock() throws IOException {
      File lockFile = getLockFile();
      return acquireGlobalLock(lockFile);
   }

   /**
    * Unlocks the current instance if holding the global lock.
    * <p>
    * This method only has an effect if the lock is hold by this instance. Effectively, the underlying file is deleted.
    * </p>
    */
   public synchronized void unlock() {
      if (globalLockFile != null) {
         if (globalLock != null && globalLock.isValid())
            Util.close(globalLock);

         globalLock = null;
         Util.close(globalLockFile);
         getLockFile().delete();
      }
   }

   /**
    * Unsafely forces the current instance to hold the lock.
    *
    * <p>
    * This method bypasses the existing lock mechanism to delete the underlying file and acquire ownership over it.
    * <b>Use this method with caution!</b> This method is useful in cases of hard crashes of the virtual machine where
    * the lock was not released prior to shutdown.
    * </p>
    *
    * @throws IOException In case of I/O errors while acquiring the lock.
    * @see #tryLock() Exceptions list.
    */
   public synchronized void unsafeLock() throws IOException {
      boolean retry;
      do {
         getLockFile().delete();
         retry = !tryLock();
      } while (retry);
   }

   /**
    * Check whether the current instance holds the global lock.
    *
    * @return <code>true</code> in case the lock is held, <code>false</code>, otherwise.
    */
   public synchronized boolean isAcquired() {
      return globalLock != null && globalLock.isValid();
   }

   private File getLockFile() {
      return directory.resolve(lockFileName()).toFile();
   }

   private boolean acquireGlobalLock(File lockFile) throws IOException {
      assert Thread.holdsLock(this);

      lockFile.getParentFile().mkdirs();
      globalLockFile = new FileOutputStream(lockFile);

      try {
         globalLock = globalLockFile.getChannel().tryLock();
         return globalLock != null && globalLock.isValid();
      } catch (OverlappingFileLockException ignore) {
         return false;
      }
   }

   private String lockFileName() {
      return String.format("%s.lck", name);
   }

   @Override
   public String toString() {
      return "FileSystemLock{directory=" + directory + ", file=" + lockFileName() + "}";
   }
}
