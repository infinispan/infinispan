package org.infinispan.client.hotrod;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class WorkerThread extends Thread {

   private static final AtomicInteger WORKER_INDEX = new AtomicInteger();

   private static Log log = LogFactory.getLog(WorkerThread.class);

   public static final String NULL = "_null_";
   public static final String PUT_SYNC = "_put_sync_";
   public static final String PUT_ASYNC = "_put_async_";

   final RemoteCache<String, String> remoteCache;
   final BlockingQueue send = new ArrayBlockingQueue(1);
   final BlockingQueue receive = new ArrayBlockingQueue(1);

   volatile String key;
   volatile String value;

   public WorkerThread(RemoteCache<String, String> remoteCache) {
      super("WorkerThread-" + WORKER_INDEX.getAndIncrement());
      this.remoteCache = remoteCache;
      start();
   }

   @Override
   public void run() {
      while (true) {
         try {
            Object o = send.take();
            trace("Took from queue: " + o);
            if (o instanceof Integer) {
               receive.put(1);
               trace("exiting!");
               return;
            }
            Object result = remoteCache.put(key, value);
            trace("Added to the cache (" + key + "," + value + ") and returning " + result);
            if (PUT_SYNC.equals(o)) {
               receive.put(result == null ? NULL : result);
               trace("Que now has: " + receive.peek());
            }
         } catch (InterruptedException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
         }
      }
   }

   /**
    * Only returns when this thread added the given key value.
    */
   public String put(String key, String value) {
      this.key = key;
      this.value = value;
      try {
         trace("::put::send contains: " + send.peek());
         send.put(PUT_SYNC);
      } catch (InterruptedException e) {
         throw new IllegalStateException(e);
      }
      try {
         String result = (String) receive.take();
         trace("::put::took out of receive: " + result);
         return result == NULL ? null : result;
      } catch (InterruptedException e) {
         throw new IllegalStateException(e);
      }
   }

   /**
    * Only returns when this thread added the given key value.
    */
   public void putAsync(String key, String value) {
      this.key = key;
      this.value = value;
      try {
         trace("::putAsync::send contains: " + send.peek());
         send.put(PUT_ASYNC);
      } catch (InterruptedException e) {
         throw new IllegalStateException(e);
      }
   }

   /**
    * Only returns when this thread is stopped.
    */
   public void stopThread() {
      try {
         send.put(new Integer(1));
         Object o = receive.take();
         trace("::stopThread::took out of receive: " + o);
      } catch (InterruptedException e) {
         e.printStackTrace();
         throw new IllegalStateException(e);
      }
   }

   private void trace(String message) {
      log.trace("Worker: " + message);
   }
}
