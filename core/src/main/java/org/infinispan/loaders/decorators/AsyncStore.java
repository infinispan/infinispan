package org.infinispan.loaders.decorators;

import net.jcip.annotations.GuardedBy;

import org.infinispan.CacheException;
import org.infinispan.Cache;
import org.infinispan.marshall.Marshaller;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.modifications.Clear;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.PurgeExpired;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
 * Write operations affecting same key are now coalesced so that only the final state is actually stored.
 * <p/>
 *
 * @author Manik Surtani
 * @author Galder Zamarreño 
 * @since 4.0
 */
public class AsyncStore extends AbstractDelegatingStore {
   private static final Log log = LogFactory.getLog(AsyncStore.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final AtomicInteger threadId = new AtomicInteger(0);
   private final AtomicBoolean stopped = new AtomicBoolean(true);
   private final AsyncStoreConfig asyncStoreConfig;
   
   /** Approximate count of number of modified keys. At points, it could contain negative values. */
   private final AtomicInteger count = new AtomicInteger(0);
   private final ReentrantLock lock = new ReentrantLock();
   private final Condition notEmpty  = lock.newCondition();

   private ExecutorService executor;
   private List<Future> processorFutures;
   private final ReadWriteLock mapLock = new ReentrantReadWriteLock();
   private final Lock read = mapLock.readLock();
   private final Lock write = mapLock.writeLock();
   private int concurrencyLevel;
   @GuardedBy("mapLock") private ConcurrentMap<Object, Modification> state;
   
   public AsyncStore(CacheStore delegate, AsyncStoreConfig asyncStoreConfig) {
      super(delegate);
      this.asyncStoreConfig = asyncStoreConfig;
   }

   @Override
   public void init(CacheLoaderConfig config, Cache cache, Marshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      concurrencyLevel = cache == null || cache.getConfiguration() == null ? 16 : cache.getConfiguration().getConcurrencyLevel();
   }

   @Override
   public void store(InternalCacheEntry ed) {
      enqueue(ed.getKey(), new Store(ed));
   }

   @Override
   public boolean remove(Object key) {
      enqueue(key, new Remove(key));
      return true;
   }

   @Override
   public void clear() {
      Clear clear = new Clear();
      enqueue(clear, clear);
   }

   @Override
   public void purgeExpired() {
      PurgeExpired purge = new PurgeExpired();
      enqueue(purge, purge);
   }
   
   @Override
   public void start() throws CacheLoaderException {
      state = newStateMap();
      log.info("Async cache loader starting {0}", this);
      stopped.set(false);
      super.start();
      int poolSize = asyncStoreConfig.getThreadPoolSize();
      executor = Executors.newFixedThreadPool(poolSize, new ThreadFactory() {
         public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "CoalescedAsyncStore-" + threadId.getAndIncrement());
            t.setDaemon(true);
            return t;
         }
      });
      processorFutures = new ArrayList<Future>(poolSize);
      for (int i = 0; i < poolSize; i++) processorFutures.add(executor.submit(createAsyncProcessor()));
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
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
      executor = null;
      super.stop();
   }
   
   protected void applyModificationsSync(ConcurrentMap<Object, Modification> mods) throws CacheLoaderException {
      Set<Map.Entry<Object, Modification>> entries = mods.entrySet();
      for (Map.Entry<Object, Modification> entry : entries) {
         Modification mod = entry.getValue();
         switch (mod.getType()) {
            case STORE:
               super.store(((Store)mod).getStoredEntry());
               break;
            case REMOVE:
               super.remove(entry.getKey());
               break;
            case CLEAR:
               super.clear();
               break;
            case PURGE_EXPIRED:
               super.purgeExpired();
               break;
         }
      }      
   }
   
   protected Runnable createAsyncProcessor() {
      return new AsyncProcessor();
   }
   
   private void enqueue(Object key, Modification mod) {
      try {
         if (stopped.get()) {
            throw new CacheException("AsyncStore stopped; no longer accepting more entries.");
         }
         if (trace) log.trace("Enqueuing modification {0}", mod);
         Modification prev = null;
         int c = -1;
         boolean unlock = false;      
         try {
            acquireLock(read);
            unlock = true;
            prev = state.put(key, mod); // put the key's latest state in updates
         } finally {
            if (unlock) read.unlock();
         }
         /* Increment can happen outside the lock cos worst case scenario a false not empty would 
          * be sent if the swap and decrement happened between the put and the increment. In this 
          * case, the corresponding processor would see the map empty and would wait again. This 
          * means that we're allowing count to potentially go negative but that's not a problem. */  
         if (prev == null) c = count.getAndIncrement();
         if (c == 0) signalNotEmpty();
      } catch (Exception e) {
         throw new CacheException("Unable to enqueue asynchronous task", e);
      }
   }

   private void acquireLock(Lock lock) {
      try {
         if (!lock.tryLock(asyncStoreConfig.getMapLockTimeout(), TimeUnit.MILLISECONDS))
            throw new CacheException("Unable to acquire lock on update map");
      } catch (InterruptedException ie) {
         // restore interrupted status
         Thread.currentThread().interrupt();
      }
   }
   
   private void signalNotEmpty() {
      lock.lock();
      try {
          notEmpty.signal();
      } finally {
          lock.unlock();
      }
   }
   
   private void awaitNotEmpty() throws InterruptedException {
      lock.lockInterruptibly();
      try {
         try {
            while (count.get() == 0)
                notEmpty.await();
         } catch (InterruptedException ie) {
            notEmpty.signal(); // propagate to a non-interrupted thread
            throw ie;
         }         
      } finally {
         lock.unlock();
      }
   }
   
   private int decrementAndGet(int delta) {
      for (;;) {
         int current = count.get();
         int next = current - delta;
         if (count.compareAndSet(current, next)) return next;
     }
   }
   
   /**
    * Processes modifications taking the latest updates from a state map.
    */
   class AsyncProcessor implements Runnable {
      private ConcurrentMap<Object, Modification> swap = newStateMap();
      
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
            if (trace) log.trace("Process remaining batch {0}", swap.size());
            put(swap);
            if (trace) log.trace("Process remaining queued {0}", state.size());
            while (!state.isEmpty()) run0();
         } catch (InterruptedException e) {
            if (trace) log.trace("Remaining interrupted");
         }
      }
      
      void run0() throws InterruptedException {
         if (trace) log.trace("Checking for modifications");
         boolean unlock = false;
         try {
            acquireLock(write);
            unlock = true;
            swap = state;
            state = newStateMap();
         } finally {
            if (unlock) write.unlock();
         }
         
         int size = swap.size();
         if (size == 0) 
            awaitNotEmpty();
         else 
            decrementAndGet(size);

         if (trace) log.trace("Calling put(List) with {0} modifications", size);
         put(swap);
      }
      
      void put(ConcurrentMap<Object, Modification> mods) {
         try {
            AsyncStore.this.applyModificationsSync(mods);
         } catch (Exception e) {
            if (log.isWarnEnabled()) log.warn("Failed to process async modifications", e);
            if (log.isDebugEnabled()) log.debug("Exception: ", e);
         }
      }
   }

   private ConcurrentMap<Object, Modification> newStateMap() {
      return new ConcurrentHashMap<Object, Modification>(64, 0.75f, concurrencyLevel);
   }
}
