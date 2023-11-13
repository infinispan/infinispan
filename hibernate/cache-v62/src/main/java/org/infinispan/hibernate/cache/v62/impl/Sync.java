package org.infinispan.hibernate.cache.v62.impl;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.hibernate.cache.spi.CacheTransactionSynchronization;
import org.hibernate.cache.spi.RegionFactory;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;

public class Sync implements CacheTransactionSynchronization {
   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(Sync.class);

   private final RegionFactory regionFactory;
   private long transactionStartTimestamp;
   // to save some allocations we're storing everything in a single array
   private Object[] tasks;
   private int index;

   public Sync(RegionFactory regionFactory) {
      this.regionFactory = regionFactory;
      transactionStartTimestamp = regionFactory.nextTimestamp();
   }

   public void registerBeforeCommit(CompletableFuture<?> future) {
      add(future);
   }

   public void registerAfterCommit(Invocation invocation) {
      assert !(invocation instanceof CompletableFuture) : "Invocation must not extend CompletableFuture";
      add(invocation);
   }

   private void add(Object task) {
      log.tracef("Adding %08x %s", System.identityHashCode(task), task);
      if (tasks == null) {
         tasks = new Object[4];
      } else if (index == tasks.length) {
         tasks = Arrays.copyOf(tasks, tasks.length * 2);
      }
      tasks[index++] = task;
   }

   // Don't put override so it is compatible with both 6.2 and 6.3
   public long getCurrentTransactionStartTimestamp() {
      return transactionStartTimestamp;
   }

   @Override
   public long getCachingTimestamp() {
      return transactionStartTimestamp;
   }

   @Override
   public void transactionJoined() {
      transactionStartTimestamp = regionFactory.nextTimestamp();
   }

   @Override
   public void transactionCompleting() {
      if (log.isTraceEnabled()) {
         int done = 0, notDone = 0;
         for (int i = 0; i < index; ++i) {
            Object task = tasks[i];
            if (task instanceof CompletableFuture) {
               if (((CompletableFuture) task).isDone()) {
                  done++;
               } else {
                  notDone++;
               }
            }
         }
         log.tracef("%d tasks done, %d tasks not done yet", done, notDone);
      }
      int count = 0;
      for (int i = 0; i < index; ++i) {
         Object task = tasks[i];
         if (task instanceof CompletableFuture) {
            log.tracef("Waiting for %08x %s", System.identityHashCode(task), task);
            try {
               ((CompletableFuture) task).join();
            } catch (CompletionException e) {
               log.debugf("Unable to complete task %08x before commit, rethrow exception", System.identityHashCode(task));
               throw e;
            }
            tasks[i] = null;
            ++count;
         } else {
            log.tracef("Not waiting for %08x %s", System.identityHashCode(task), task);
         }
      }
      if (log.isTraceEnabled()) {
         log.tracef("Finished %d tasks before completion", count);
      }
   }

   @Override
   public void transactionCompleted(boolean successful) {
      if (!successful) {
         // When the transaction is rolled back transactionCompleting() is not called,
         // so we could have some completable futures waiting.
         transactionCompleting();
      }
      int invoked = 0, waiting = 0;
      for (int i = 0; i < index; ++i) {
         Object invocation = tasks[i];
         if (invocation == null) {
            continue;
         }
         try {
            tasks[i] = ((Invocation) invocation).invoke(successful);
         } catch (Exception e) {
            log.failureAfterTransactionCompletion(i, successful, e);
            tasks[i] = null;
         }
         invoked++;
      }
      for (int i = 0; i < index; ++i) {
         CompletableFuture<?> cf = (CompletableFuture<?>) tasks[i];
         if (cf != null) {
            try {
               cf.join();
            } catch (Exception e) {
               log.failureAfterTransactionCompletion(i, successful, e);
            }
            waiting++;
         }
      }
      if (log.isTraceEnabled()) {
         log.tracef("Invoked %d tasks after completion, %d are synchronous.", invoked, waiting);
      }
   }
}
