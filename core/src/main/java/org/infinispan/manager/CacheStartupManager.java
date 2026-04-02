package org.infinispan.manager;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ProgressTracker;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Orchestrates cache startup during container initialization.
 *
 * <p>
 * Internal caches are started serially and must all succeed before the cache manager becomes {@code RUNNING}. User caches
 * are submitted concurrently to the blocking executor and start in the background. The cache manager does not wait for user
 * caches to initialize.
 * </p>
 *
 * @since 16.2
 * @author José Bolina
 */
@Scope(Scopes.GLOBAL)
public class CacheStartupManager {

   private static final Log LOG = LogFactory.getLog(CacheStartupManager.class);

   @Inject
   protected EmbeddedCacheManager cacheManager;

   @Inject
   protected BlockingManager blockingManager;

   @Inject
   protected TimeService timeService;

   @Inject
   @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   protected ScheduledExecutorService timeoutExecutor;

   private final Map<String, CacheStartupState> states = new ConcurrentHashMap<>();
   private final AtomicInteger pending = new AtomicInteger();
   private volatile boolean stopped;
   private volatile ProgressTracker progressTracker;

   /**
    * Starts internal caches serially. Blocks until all caches are running.
    *
    * <p>
    * Failure is fatal — the exception propagates and the cache manager shuts down.
    * </p>
    *
    * @param names the names of the internal caches to start. Must not be {@code null}.
    * @throws RuntimeException if any internal cache fails to start
    * @throws NullPointerException if the provided names are {@code null}
    */
   public void startInternalCaches(Set<String> names) {
      Objects.requireNonNull(names, "Cache names can not be null");
      LOG.debugf("Starting internal caches serially: %s", names);
      for (String name : names) {
         SecurityActions.getCache(cacheManager, name);
      }
   }

   /**
    * Submits user caches for concurrent startup via the blocking executor.
    *
    * <p>
    * Returns immediately (fire-and-forget). Can be called multiple times and accumulate the tasks across calls. The
    * {@link ProgressTracker} tracks the total count and progress.
    * </p>
    *
    * @param names the names of the user caches to start. Must not be {@code null}.
    * @throws NullPointerException In case the provided names are {@code null}.
    */
   public void startUserCaches(Set<String> names) {
      Objects.requireNonNull(names, "Cache names can not be null");
      if (stopped || names.isEmpty()) return;

      initProgressTracker();
      for (String name : names) {
         states.put(name, CacheStartupState.STARTING);
      }

      pending.addAndGet(names.size());
      progressTracker.addTasks(names.size());

      for (String name : names) {
         blockingManager.runBlocking(new CacheStartupRunnable(name), "cache-start-" + name);
      }
   }

   /**
    * Returns the startup state of a specific cache.
    *
    * @param name the cache name to query. Must not be {@code null}.
    * @return the current {@link CacheStartupState}, or {@code null} if the cache is not tracked by this manager
    */
   public CacheStartupState getState(String name) {
      return states.get(name);
   }

   /**
    * Returns an unmodifiable snapshot of all tracked caches and their states.
    *
    * @return a map from cache name to {@link CacheStartupState}. Never {@code null}.
    */
   public Map<String, CacheStartupState> getAllStates() {
      return Collections.unmodifiableMap(states);
   }

   /**
    * Signals shutdown.
    *
    * <p>
    * Prevents new cache submissions and causes pending tasks to complete with {@link CacheStartupState#FAILED}.
    * </p>
    */
   @Stop
   public void stop() {
      stopped = true;
      if (progressTracker != null)
         progressTracker.finishedAllTasks();
   }

   private void initProgressTracker() {
      if (progressTracker == null) {
         synchronized (this) {
            if (progressTracker == null) {
               progressTracker = new ProgressTracker("cache-startup", timeoutExecutor, timeService, 30, TimeUnit.SECONDS);
            }
         }
      }
   }

   private final class CacheStartupRunnable implements Runnable {
      private final String name;

      private CacheStartupRunnable(String name) {
         this.name = name;
      }

      @Override
      public void run() {
         if (stopped) {
            taskCompleted(CacheStartupState.FAILED);
            return;
         }

         try {
            LOG.debugf("Requesting cache %s to start", name);
            SecurityActions.getCache(cacheManager, name);
            taskCompleted(CacheStartupState.READY);
         } catch (Throwable t) {
            LOG.errorf(t, "Failed to start cache %s", name);
            taskCompleted(CacheStartupState.FAILED);
         }
      }

      private void taskCompleted(CacheStartupState state) {
         states.put(name, state);
         progressTracker.removeTasks(1);
         if (pending.decrementAndGet() == 0) {
            progressTracker.finishedAllTasks();
         }
      }
   }
}
