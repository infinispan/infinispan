package org.infinispan.lucene.impl;

import java.io.IOException;

import org.apache.lucene.store.LockObtainFailedException;

public class CommonLockObtainUtils {

   private static final int MAX_LOCK_ACQUIRE_MILLISECONDS = 10;

   private CommonLockObtainUtils() {
      //not to be constructed
   }

   public static void attemptObtain(ObtainableLock lock) throws IOException {
      int attempts = 0;
      while (!lock.obtain()) {
         //we could fail immediately, but being Infinispan a bit latency sensitive we give a bit of grace here
         if (attempts++ > MAX_LOCK_ACQUIRE_MILLISECONDS)
            failLockAcquire();
         if (Thread.currentThread().isInterrupted())
            failLockAcquire();
         try {
            Thread.sleep(1);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failLockAcquire();
         }
      }
   }

   private static void failLockAcquire() throws LockObtainFailedException {
      throw new LockObtainFailedException("lock instance already assigned");
   }

}
