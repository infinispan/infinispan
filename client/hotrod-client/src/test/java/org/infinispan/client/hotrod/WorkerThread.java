package org.infinispan.client.hotrod;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Mircea.Markus@jboss.com
 * @author dberinde@redhat.com
 * @since 4.1
 */
public class WorkerThread {

   private static final AtomicInteger WORKER_INDEX = new AtomicInteger();

   private static final Log log = LogFactory.getLog(WorkerThread.class);

   private final RemoteCache remoteCache;
   private volatile Future<?> future;

   private final ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
      public Thread newThread(Runnable r) {
         return new Thread(r, String.format("%s-Worker-%d",
               Thread.currentThread().getName(), WORKER_INDEX.getAndIncrement()));
      }
   });

   public WorkerThread(RemoteCache remoteCache) {
      this.remoteCache = remoteCache;
   }

   private void stressInternal() throws Exception {
      Random rnd = new Random();
      while (!executor.isShutdown()) {
         remoteCache.put(rnd.nextLong(), rnd.nextLong());
         Thread.sleep(50);
      }
   }

   /**
    * Only returns when this thread added the given key value.
    */
   public String put(final String key, final String value) {
      Future<?> result = executor.submit(new Callable<Object>() {
         public Object call() {
            return remoteCache.put(key, value);
         }
      });

      try {
         return (String) result.get();
      } catch (InterruptedException e) {
         throw new IllegalStateException();
      } catch (ExecutionException e) {
         throw new RuntimeException("Error during put", e.getCause());
      }
   }

   /**
    * Does a put on the worker thread.
    * Doesn't wait for the put operation to finish. However, it will wait for the previous operation on this thread to finish.
    */
   public Future<?> putAsync(final String key, final String value) throws ExecutionException, InterruptedException {
      if (future != null) {
         future.get();
      }
      return executor.submit(new Callable<Object>() {
         public Object call() throws Exception {
            return remoteCache.put(key, value);
         }
      });
   }

   /**
    * Starts doing cache put operations in a loop on the worker thread.
    * Doesn't wait for the loop to finish - in fact the loop will finish only when the worker is stopped.
    * However, it will wait for the previous operation on this thread to finish.
    */
   public Future<?> stress() throws InterruptedException, ExecutionException {
      if (future != null) {
         future.get();
      }
      return executor.submit(new Callable<Object>() {
         public Object call() throws Exception {
            stressInternal();
            return null;
         }
      });
   }

   /**
    * Returns without waiting for the threads to finish.
    */
   public void stop() {
      executor.shutdown();
   }

   /**
    * Only returns when the last operation on this thread has finished.
    */
   public void awaitTermination() throws InterruptedException, ExecutionException {
      executor.awaitTermination(1, TimeUnit.SECONDS);
      if (future != null) {
         future.get();
      }
   }
}
