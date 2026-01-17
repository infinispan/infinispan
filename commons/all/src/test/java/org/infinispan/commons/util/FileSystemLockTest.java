package org.infinispan.commons.util;

import static org.infinispan.testing.Testing.tmpDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.concurrent.FileSystemLock;
import org.junit.After;
import org.junit.Test;

public class FileSystemLockTest {

   @After
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

   private static String directoryName() {
      return FileSystemLockTest.class.getSimpleName();
   }
}
