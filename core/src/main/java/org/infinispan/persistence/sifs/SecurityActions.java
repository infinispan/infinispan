package org.infinispan.persistence.sifs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.security.Security;

/**
 * SecurityActions for the org.infinispan.persistence.sifs package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author William Burns
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

   static <T> T doPrivilegedException(PrivilegedExceptionAction<T> action) throws FileNotFoundException {
      try {
         if (System.getSecurityManager() != null) {
            return AccessController.doPrivileged(action);
         } else {
            return Security.doPrivileged(action);
         }
      } catch (PrivilegedActionException e) {
         if (e.getCause() instanceof FileNotFoundException) {
            throw ((FileNotFoundException) e.getCause());
         } else {
            throw new CacheConfigurationException(e);
         }
      }
   }

   static boolean fileExists(File file) {
      return doPrivileged(file::exists);
   }

   static FileChannel createChannel(File file) throws FileNotFoundException {
      return doPrivilegedException(() -> new FileOutputStream(file).getChannel());
   }

   static FileChannel openFileChannel(File file) throws FileNotFoundException {
      return doPrivilegedException(() -> new RandomAccessFile(file, "rw").getChannel());
   }

   public static boolean createDirectoryIfNeeded(File dir) {
      return doPrivileged(() -> dir.mkdirs() || dir.exists());
   }
}
