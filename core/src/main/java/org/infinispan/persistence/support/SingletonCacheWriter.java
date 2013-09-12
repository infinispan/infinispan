package org.infinispan.persistence.support;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.persistence.PersistenceUtil.internalMetadata;

/**
 * SingletonStore is a delegating cache store used for situations when only one instance should interact with the
 * underlying store. The coordinator of the cluster will be responsible for the underlying CacheStore.
 * <p/>
 * SingletonStore is a simply facade to a real CacheStore implementation. It always delegates reads to the real
 * CacheStore.
 * <p/>
 * Writes are delegated <i>only if</i> this SingletonStore is currently the coordinator. This avoids having all stores in
 * a cluster writing the same data to the same underlying store. Although not incorrect (e.g. a DB will just discard
 * additional INSERTs for the same key, and throw an exception), this will avoid a lot of redundant work.
 * <p/>
 * Whenever the current coordinator dies (or leaves), the second in line will take over. That SingletonStore will then
 * pass writes through to its underlying CacheStore. Optionally, when a new coordinator takes over the Singleton, it can
 * push the in-memory state to the cache cacheStore, within a time constraint.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public class SingletonCacheWriter extends DelegatingCacheWriter {

   private static final Log log = LogFactory.getLog(SingletonCacheWriter.class);
   private static final boolean trace = log.isTraceEnabled();

   private SingletonStoreConfiguration singletonConfiguration;

   /**
    * Name of thread that should pushing in-memory state to cache loader.
    */
   private static final String THREAD_NAME = "SingletonStorePusherThread";

   /**
    * Executor service used to submit tasks to push in-memory state.
    */
   protected final ExecutorService executor;

   /**
    * Future result of the in-memory push state task. This allows SingletonStore to check whether there's any push taks
    * on going.
    */
   Future<?> pushStateFuture; /* FutureTask guarantees a safe publication of the result */

   /**
    * Address instance that allows SingletonStore to find out whether it became the coordinator of the cluster, or
    * whether it stopped being it. This dictates whether the SingletonStore is active or not.
    */
   private Address localAddress;

   /**
    * Whether the the current cache is the coordinator and therefore SingletonStore is active. Being active means
    * delegating calls to the underlying cache loader.
    */
   protected volatile boolean active;


   public SingletonCacheWriter(CacheWriter actual, SingletonStoreConfiguration singletonConfiguration) {
      super(actual);
      executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            return new Thread(r, THREAD_NAME);
         }
      });
      this.singletonConfiguration = singletonConfiguration;
   }

   @Override
   public void start() {
      ctx.getCache().getCacheManager().addListener(new SingletonStoreListener());
   }

   @Override
   public void stop() {
      executor.shutdownNow();
   }

   @Override
   public void write(MarshalledEntry entry) {
      if (active) {
         if (trace)
            log.tracef("Storing key %s.  Instance: %s", entry.getKey(), this);
         super.write(entry);
      } else {
        if (trace)
           log.tracef("Not storing key %s.  Instance: %s", entry.getKey(), this);
      }
   }

   @Override
   public boolean delete(Object key) {
      return active && super.delete(key);
   }

   protected Callable<?> createPushStateTask() {
      return new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            final boolean debugEnabled = log.isDebugEnabled();

            if (debugEnabled) log.debug("start pushing in-memory state to cache cacheLoader");
            pushState(ctx.getCache());
            if (debugEnabled) log.debug("in-memory state passed to cache cacheLoader successfully");

            return null;
         }
      };
   }

   /**
    * Pushes the state of a specific cache by reading the cache's data and putting in the cache store.
    */
   protected void pushState(final Cache cache) throws Exception {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      Set<Object> keys = dc.keySet();
      for (Object k : keys) {
         InternalCacheEntry entry = dc.get(k);
         if (entry != null) {
            MarshalledEntry me = ctx.getMarshalledEntryFactory().newMarshalledEntry(entry.getKey(), entry.getValue(),
                                                             internalMetadata(entry));
            write(me);
         }
      }
   }


   /**
    * Method that waits for the in-memory to cache loader state to finish. This method's called in case a push state is
    * already in progress and we need to wait for it to finish.
    */
   protected void awaitForPushToFinish(Future<?> future, long timeout, TimeUnit unit) {
      final boolean debugEnabled = log.isDebugEnabled();
      try {
         if (debugEnabled) log.debug("wait for state push to cache loader to finish");
         future.get(timeout, unit);
      } catch (TimeoutException e) {
         if (debugEnabled) log.debug("timed out waiting for state push to cache loader to finish");
      } catch (ExecutionException e) {
         if (debugEnabled) log.debug("exception reported waiting for state push to cache loader to finish");
      } catch (InterruptedException ie) {
         /* Re-assert the thread's interrupted status */
         Thread.currentThread().interrupt();
         if (trace) log.trace("wait for state push to cache loader to finish was interrupted");
      }
   }


   /**
    * Method called when the cache either becomes the coordinator or stops being the coordinator. If it becomes the
    * coordinator, it can optionally start the in-memory state transfer to the underlying cache store.
    */
   protected void activeStatusChanged(boolean newActiveState) throws PushStateException {
      active = newActiveState;
      log.debugf("changed mode %s", this);
      if (active && singletonConfiguration.pushStateWhenCoordinator())
         doPushState();
   }

   private boolean isCoordinator(List<Address> newView, Address currentAddress) {
      if (!currentAddress.equals(localAddress)) localAddress = currentAddress;
      return !newView.isEmpty() && localAddress.equals(newView.get(0));
   }

   /**
    * Called when the SingletonStore discovers that the cache has become the coordinator and push in memory state has
    * been enabled. It might not actually push the state if there's an ongoing push task running, in which case will
    * wait for the push task to finish.
    */
   private void doPushState() throws PushStateException {
      if (pushStateFuture == null || pushStateFuture.isDone()) {
         Callable<?> task = createPushStateTask();
         pushStateFuture = executor.submit(task);
         try {
            waitForTaskToFinish(pushStateFuture, singletonConfiguration.pushStateTimeout(), TimeUnit.MILLISECONDS);
         } catch (Exception e) {
            throw new PushStateException("unable to complete in memory state push to cache loader", e);
         }
      } else {
         /* at the most, we wait for push state timeout value. if it push task finishes earlier, this call
* will stop when the push task finishes, otherwise a timeout exception will be reported */
         awaitForPushToFinish(pushStateFuture, singletonConfiguration.pushStateTimeout(), TimeUnit.MILLISECONDS);
      }
   }


   /**
    * Waits, within a time constraint, for a task to finish.
    */
   private void waitForTaskToFinish(Future<?> future, long timeout, TimeUnit unit) throws Exception {
      try {
         future.get(timeout, unit);
      } catch (TimeoutException e) {
         throw new Exception("task timed out", e);
      } catch (InterruptedException e) {
         /* Re-assert the thread's interrupted status */
         Thread.currentThread().interrupt();
         if (trace) log.trace("task was interrupted");
      } finally {
         /* no-op if task is completed */
         future.cancel(true); /* interrupt if running */
      }
   }


   /**
    * Cache listener that reacts to cluster topology changes to find out whether a new coordinator is elected.
    * SingletonStore reacts to these changes in order to decide which cache should interact with the underlying cache
    * store.
    */
   @Listener
   public class SingletonStoreListener {
      /**
       * Cache started, check whether the cache is the coordinator and set the singleton store's active status.
       */
      @CacheStarted
      public void cacheStarted(Event e) {
         EmbeddedCacheManager cm = ctx.getCache().getCacheManager();
         localAddress = cm.getAddress();
         active = cm.isCoordinator();
      }

      /**
       * The cluster formation changed, so determine whether the current cache stopped being the coordinator or became
       * the coordinator. This method can lead to an optional in memory to cache loader state push, if the current cache
       * became the coordinator. This method will report any issues that could potentially arise from this push.
       */
      @ViewChanged
      public void viewChange(ViewChangedEvent event) {
         boolean tmp = isCoordinator(event.getNewMembers(), event.getLocalAddress());

         if (active != tmp) {
            try {
               activeStatusChanged(tmp);
            } catch (PushStateException e) {
               log.errorChangingSingletonStoreStatus(e);
            }

         }
      }
   }

   /**
    * Exception representing any issues that arise from pushing the in-memory state to the cache loader.
    */
   public static class PushStateException extends Exception {
      private static final long serialVersionUID = 5542893943730200886L;

      public PushStateException(String message, Throwable cause) {
         super(message, cause);
      }
   }

   @Override
   public String toString() {
      return "SingletonStore: localAddress=" + localAddress + ", active=" + active;
   }

}
