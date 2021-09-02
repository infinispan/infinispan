package org.infinispan.persistence.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.security.Security;

/**
 * SecurityActions for the org.infinispan.persistence.file package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Dan Berindei
 * @since 13.0
 */
final class SecurityActions {
   static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static <T> T doPrivilegedException(PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static boolean fileExists(File file) {
      return doPrivileged(file::exists);
   }

   static boolean deleteFile(File file) {
      return doPrivileged(file::delete);
   }

   static void moveFile(Path sourcePath, Path destPath, StandardCopyOption... moveOptions) throws IOException {
      try {
         doPrivilegedException(() -> Files.move(sourcePath, destPath, moveOptions));
      } catch (PrivilegedActionException e) {
         if (e.getCause() instanceof IOException) {
            throw ((IOException) e.getCause());
         } else {
            throw new PersistenceException(e);
         }
      }
   }

   static FileChannel openFileChannel(File file) throws FileNotFoundException {
      try {
         return doPrivilegedException(() -> new RandomAccessFile(file, "rw").getChannel());
      } catch (PrivilegedActionException e) {
         if (e.getCause() instanceof FileNotFoundException) {
            throw ((FileNotFoundException) e.getCause());
         } else {
            throw new PersistenceException(e);
         }
      }
   }

   public static boolean createDirectoryIfNeeded(File dir) {
      return doPrivileged(() -> dir.mkdirs() || dir.exists());
   }
}
