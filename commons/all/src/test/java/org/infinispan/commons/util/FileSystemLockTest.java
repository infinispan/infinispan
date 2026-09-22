package org.infinispan.commons.util;

import static org.infinispan.testing.Testing.tmpDirectory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.concurrent.FileSystemLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class FileSystemLockTest {

   @AfterEach
   public void afterEach() {
      Util.recursiveFileRemove(tmpDirectory(directoryName()));
   }

   @Test
   public void testLockAndUnlock() throws Exception {
      String path = tmpDirectory(directoryName());

      FileSystemLock lock = new FileSystemLock(Paths.get(path), "test-1");

      assertTrue(lock.tryLock());
      assertTrue(lock.isAcquired());

      assertFalse(lock.tryLock());

      lock.unlock();
      assertFalse(lock.isAcquired());
   }

   @Test
   public void testMultipleThreads() throws Exception {
      String path = tmpDirectory(directoryName());
      ExecutorService executor = Executors.newFixedThreadPool(4);
      try {
         List<CompletableFuture<?>> futures = new ArrayList<>();

         CyclicBarrier barrier = new CyclicBarrier(5);
         ByRef.Integer counter = new ByRef.Integer(0);
         FileSystemLock lock = new FileSystemLock(Paths.get(path), "test-2");

         for (int i = 0; i < 4; i++) {
            futures.add(CompletableFuture.supplyAsync(() -> {
               try {
                  barrier.await(10, TimeUnit.SECONDS);
                  if (lock.tryLock()) {
                     counter.inc();
                  }
               } catch (Exception e) {
                  throw new RuntimeException(e);
               }
               return null;
            }, executor));
         }

         barrier.await(10, TimeUnit.SECONDS);
         futures.forEach(CompletableFuture::join);

         assertEquals(1, counter.get());

         // Assert it is still locked.
         assertFalse(lock.tryLock());
         lock.unlock();
      } finally {
         executor.shutdown();
      }
   }

   @Test
   public void testUnsafeLocking() throws Exception {
      String path = tmpDirectory(directoryName());

      FileSystemLock lock = new FileSystemLock(Paths.get(path), "test-3");

      assertTrue(lock.tryLock());
      assertTrue(lock.isAcquired());

      lock.unsafeLock();
   }

   @Test
   public void testLockNameWithSlashes() throws Exception {
      String path = tmpDirectory(directoryName());

      // Lock name with slashes, like a cache name "test/cache/with/slashes"
      FileSystemLock lock = new FileSystemLock(Paths.get(path), "test/cache/with/slashes");

      // Should successfully acquire lock despite slashes in the name
      assertTrue(lock.tryLock());
      assertTrue(lock.isAcquired());

      // Should be able to check if locked
      assertFalse(lock.tryLock());

      // Verify no subdirectories were created.
      // The lock file should be directly in the base directory
      File lockDir = new File(path);
      File[] files = lockDir.listFiles();
      assertNotNull(files);
      assertEquals(1, files.length, "Should have exactly one file in lock directory, not nested subdirectories");
      assertTrue(files[0].isFile(), "Should be a file, not a directory");
      assertTrue(files[0].getName().endsWith(".lck"), "Should be a .lck file");

      // Should be able to unlock
      lock.unlock();
      assertFalse(lock.isAcquired());

      // Should be able to lock again after unlock
      assertTrue(lock.tryLock());
      lock.unlock();
   }

   @Test
   public void testLockNameWithOtherProblematicChars() throws Exception {
      String path = tmpDirectory(directoryName());

      // Lock name with various problematic characters
      FileSystemLock lock = new FileSystemLock(Paths.get(path), "lock:with\\various<problem>chars");

      assertTrue(lock.tryLock());
      assertTrue(lock.isAcquired());

      lock.unlock();
      assertFalse(lock.isAcquired());
   }

   @Test
   public void testLockNameWithPathTraversal() throws Exception {
      String path = tmpDirectory(directoryName());

      // Lock name attempting path traversal
      FileSystemLock lock = new FileSystemLock(Paths.get(path), "../../escape/attempt");

      // Should successfully acquire lock
      assertTrue(lock.tryLock());
      assertTrue(lock.isAcquired());

      // Verify the lock file stays within the base directory (no path traversal)
      File lockDir = new File(path);
      File[] files = lockDir.listFiles();
      assertNotNull(files);
      assertEquals(1, files.length, "Lock file should stay in base directory");
      assertTrue(files[0].isFile());

      // Verify no files were created outside the base directory
      File parentDir = lockDir.getParentFile();
      assertFalse(new File(parentDir, "escape").exists(), "Path traversal should be prevented");

      lock.unlock();
      assertFalse(lock.isAcquired());
   }

   @Test
   public void testLockCollisionAvoidance() throws Exception {
      String path = tmpDirectory(directoryName());

      // Two different lock names that might collide if naively escaped
      FileSystemLock lock1 = new FileSystemLock(Paths.get(path), "cache/name");
      FileSystemLock lock2 = new FileSystemLock(Paths.get(path), "cache:name");

      // Both should be able to lock independently
      assertTrue(lock1.tryLock());
      assertTrue(lock2.tryLock());

      assertTrue(lock1.isAcquired());
      assertTrue(lock2.isAcquired());

      lock1.unlock();
      lock2.unlock();

      assertFalse(lock1.isAcquired());
      assertFalse(lock2.isAcquired());
   }

   private static String directoryName() {
      return FileSystemLockTest.class.getSimpleName();
   }
}
