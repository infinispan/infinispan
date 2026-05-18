package org.infinispan.manager;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ProgressTracker;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.executors.ManageableExecutorService;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.factories.threads.AbstractThreadPoolExecutorFactory;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.schedulers.Schedulers;

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

   private static final int USER_CACHE_PARALLELISM = 5;
   private static final Log LOG = LogFactory.getLog(CacheStartupManager.class);

   @Inject
   protected EmbeddedCacheManager cacheManager;

   @Inject
   protected GlobalConfiguration configuration;

   @Inject
   protected TimeService timeService;

   @Inject
   @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   protected ScheduledExecutorService timeoutExecutor;

   @Inject
   @ComponentName(KnownComponentNames.BLOCKING_EXECUTOR)
   protected ExecutorService blockingExecutor;

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
    * <p>
    * If the blocking executor thread pool is too small for concurrent startup, caches are started serially to avoid potential
    * deadlock. Cache components submit nested blocking work during {@code cache.start()}, so the pool must have enough
    * threads for both the cache start tasks and the nested work.
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

      int parallelism = resolveMaximumConcurrency();
      LOG.debugf("Starting cache parallel of %d for %s", parallelism, names);
      if (parallelism > 1) {
         CompletionStages.performConcurrently(names, parallelism, Schedulers.from(blockingExecutor), this::startCache);
      } else {
         for (String name : names) {
            startCache(name);
         }
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

   private int resolveMaximumConcurrency() {
      // Some custom executor injected externally.
      // This means the executor was not created by our internal setup, we'll play it safe and start serially.
      if (!(blockingExecutor instanceof ManageableExecutorService<?> mes)) {
         return 1;
      }

      // We have an upper bound of <PARALLELISM> concurrent starts.
      // Each request to start a cache will block, and additionally, the components can block, too.
      // This requires a minimum of 2 thread to start a single cache.
      int maxPoolSize = mes.getMaximumPoolSize();
      if (maxPoolSize > 0) {
         return Math.min(USER_CACHE_PARALLELISM, maxPoolSize / 2);
      }

      ThreadPoolExecutorFactory<?> tpef = configuration.blockingThreadPool().threadPoolFactory();
      if (tpef == null)
         return USER_CACHE_PARALLELISM;

      if (tpef instanceof AbstractThreadPoolExecutorFactory<?> f) {
         return Math.min(USER_CACHE_PARALLELISM, f.maxThreads() / 2);
      }

      // Unknown executor type or configuration.
      // We default serial execution instead of risking a deadlock.
      return 1;
   }

   private void taskCompleted(String name, CacheStartupState state) {
      states.put(name, state);
      progressTracker.removeTasks(1);
      if (pending.decrementAndGet() == 0) {
         progressTracker.finishedAllTasks();
      }
   }

   private CompletionStage<?> startCache(String name) {
      if (stopped) {
         taskCompleted(name, CacheStartupState.FAILED);
         return CompletableFutures.completedNull();
      }

      try {
         LOG.debugf("Requesting cache %s to start", name);
         SecurityActions.getCache(cacheManager, name);
         taskCompleted(name, CacheStartupState.READY);
      } catch (Throwable t) {
         LOG.errorf(t, "Failed to start cache %s", name);
         taskCompleted(name, CacheStartupState.FAILED);
      }
      return CompletableFutures.completedNull();
   }
}
