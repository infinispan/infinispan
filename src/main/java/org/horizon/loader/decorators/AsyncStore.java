package org.horizon.loader.decorators;

import org.horizon.CacheException;
import org.horizon.loader.CacheLoaderException;
import org.horizon.loader.CacheStore;
import org.horizon.loader.StoredEntry;
import org.horizon.loader.modifications.Clear;
import org.horizon.loader.modifications.Modification;
import org.horizon.loader.modifications.PurgeExpired;
import org.horizon.loader.modifications.Remove;
import org.horizon.loader.modifications.Store;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The AsyncStore is a delegating CacheStore that extends AbstractDelegatingStore, overriding methods to that should not
 * just delegate the operation to the underlying store.
 * <p/>
 * Read operations are done synchronously, while write operations are done asynchronously.  There is no provision for
 * exception handling for problems encountered with the underlying store during a write operation, and the exception is
 * just logged.
 * <p/>
 * When configuring the loader, use the following element:
 * <p/>
 * <code> &lt;async enabled="true" /&gt; </code>
 * <p/>
 * to define whether cache loader operations are to be asynchronous.  If not specified, a cache loader operation is
 * assumed synchronous and this decorator is not applied.
 * <p/>
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class AsyncStore extends AbstractDelegatingStore {

   private static final Log log = LogFactory.getLog(AsyncStore.class);
   private static final boolean trace = log.isTraceEnabled();

   private static AtomicInteger threadId = new AtomicInteger(0);

   private ExecutorService executor;
   private AtomicBoolean stopped = new AtomicBoolean(true);
   private BlockingQueue<Modification> queue;
   private List<Future> processorFutures;
   private AsyncStoreConfig asyncStoreConfig;

   public AsyncStore(CacheStore cacheStore, AsyncStoreConfig asyncStoreConfig) {
      super(cacheStore);
      this.asyncStoreConfig = asyncStoreConfig;
   }

   public void store(StoredEntry ed) {
      enqueue(new Store(ed));
   }

   public void clear() {
      enqueue(new Clear());
   }

   public boolean remove(Object key) {
      enqueue(new Remove(key));
      return true;
   }

   public void purgeExpired() {
      enqueue(new PurgeExpired());
   }

   private void enqueue(final Modification mod) {
      try {
         if (stopped.get()) {
            throw new CacheException("AsyncStore stopped; no longer accepting more entries.");
         }
         log.trace("Enqueuing modification {0}", mod);
         queue.put(mod);
      } catch (Exception e) {
         throw new CacheException("Unable to enqueue asynchronous task", e);
      }
   }

   @Override
   public void start() throws CacheLoaderException {
      queue = new LinkedBlockingQueue<Modification>(asyncStoreConfig.getQueueSize());
      log.info("Async cache loader starting {0}", this);
      stopped.set(false);
      super.start();
      int poolSize = asyncStoreConfig.getThreadPoolSize();
      executor = Executors.newFixedThreadPool(poolSize, new ThreadFactory() {
         public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "AsyncStore-" + threadId.getAndIncrement());
            t.setDaemon(true);
            return t;
         }
      });
      processorFutures = new ArrayList<Future>(poolSize);
      for (int i = 0; i < poolSize; i++) processorFutures.add(executor.submit(new AsyncProcessor()));
   }

   @Override
   public void stop() throws CacheLoaderException {
      stopped.set(true);
      if (executor != null) {
         for (Future f : processorFutures) f.cancel(true);
         executor.shutdown();
         try {
            boolean terminated = executor.isTerminated();
            while (!terminated) {
               terminated = executor.awaitTermination(60, TimeUnit.SECONDS);
            }
         }
         catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
      executor = null;
      super.stop();
   }

   protected void applyModificationsSync(List<Modification> mods) throws CacheLoaderException {
      for (Modification m : mods) {
         switch (m.getType()) {
            case STORE:
               Store s = (Store) m;
               super.store(s.getStoredEntry());
               break;
            case CLEAR:
               super.clear();
               break;
            case REMOVE:
               Remove r = (Remove) m;
               super.remove(r.getKey());
               break;
            case PURGE_EXPIRED:
               super.purgeExpired();
               break;
            default:
               throw new IllegalArgumentException("Unknown modification type " + m.getType());
         }
      }
   }


   /**
    * Processes (by batch if possible) a queue of {@link Modification}s.
    *
    * @author manik surtani
    */
   private class AsyncProcessor implements Runnable {
      // Modifications to invoke as a single put
      private final List<Modification> mods = new ArrayList<Modification>(asyncStoreConfig.getBatchSize());

      public void run() {
         while (!Thread.interrupted()) {
            try {
               run0();
            }
            catch (InterruptedException e) {
               break;
            }
         }

         try {
            if (trace) log.trace("Process remaining batch {0}", mods.size());
            put(mods);
            if (trace) log.trace("Process remaining queued {0}", queue.size());
            while (!queue.isEmpty()) run0();
         }
         catch (InterruptedException e) {
            log.trace("remaining interrupted");
         }
      }

      private void run0() throws InterruptedException {
         log.trace("Checking for modifications");
         int i = queue.drainTo(mods, asyncStoreConfig.getBatchSize());
         if (i == 0) {
            Modification m = queue.take();
            mods.add(m);
         }

         if (trace) log.trace("Calling put(List) with {0} modifications", mods.size());
         put(mods);
         mods.clear();
      }

      private void put(List<Modification> mods) {
         try {
            AsyncStore.this.applyModificationsSync(mods);
         }
         catch (Exception e) {
            if (log.isWarnEnabled()) log.warn("Failed to process async modifications: " + e);
            if (log.isDebugEnabled()) log.debug("Exception: ", e);
         }
      }
   }

}
