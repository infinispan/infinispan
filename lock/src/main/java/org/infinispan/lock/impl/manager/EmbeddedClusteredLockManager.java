package org.infinispan.lock.impl.manager;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockConfiguration;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.exception.ClusteredLockException;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockState;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.impl.lock.ClusteredLockImpl;
import org.infinispan.lock.impl.log.Log;
import org.infinispan.util.ByteString;

/**
 * The Embedded version for the lock cluster manager
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Scope(Scopes.GLOBAL)
@MBean(objectName = EmbeddedClusteredLockManager.OBJECT_NAME, description = "Component to manage clustered locks")
public class EmbeddedClusteredLockManager implements ClusteredLockManager {
   public static final String OBJECT_NAME = "ClusteredLockManager";
   private static final long WAIT_CACHES_TIMEOUT = TimeUnit.SECONDS.toNanos(15);
   private static final Log log = LogFactory.getLog(EmbeddedClusteredLockManager.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   public static final String FORCE_RELEASE = "forceRelease";
   public static final String REMOVE = "remove";
   public static final String IS_DEFINED = "isDefined";
   public static final String IS_LOCKED = "isLocked";

   private final ConcurrentHashMap<String, ClusteredLock> locks = new ConcurrentHashMap<>();
   private final CompletableFuture<CacheHolder> cacheHolderFuture;
   private ScheduledExecutorService scheduledExecutorService;
   private Executor executor;

   private AdvancedCache<ClusteredLockKey, ClusteredLockValue> cache;

   public EmbeddedClusteredLockManager(CompletableFuture<CacheHolder> cacheHolderFuture) {
      this.cacheHolderFuture = cacheHolderFuture;
   }

   @Inject
   public void injectDep(@ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService scheduledExecutorService,
                         @ComponentName(KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR) Executor executor) {
      this.scheduledExecutorService = scheduledExecutorService;
      this.executor = executor;
   }

   @Override
   public boolean defineLock(String name) {
      ClusteredLockConfiguration configuration = new ClusteredLockConfiguration();
      if (trace) {
         log.tracef("LOCK[%s] defineLock with default configuration has been called %s", name, configuration);
      }
      return defineLock(name, configuration);
   }

   @Override
   public boolean defineLock(String name, ClusteredLockConfiguration configuration) {
      if (trace) {
         log.tracef("LOCK[%s] defineLock has been called %s", name, configuration);
      }
      // TODO: Configuration is not used because we don't support any other mode for now. For that : ISPN-8413
      CacheHolder cacheHolder = extractCacheHolder(cacheHolderFuture);
      cache = cacheHolder.getClusteredLockCache();
      ClusteredLockKey key = new ClusteredLockKey(ByteString.fromString(name));
      ClusteredLockValue clusteredLockValue = cache.putIfAbsent(key, ClusteredLockValue.INITIAL_STATE);
      locks.putIfAbsent(name, new ClusteredLockImpl(name, key, cache, this));
      return clusteredLockValue == null;
   }

   @Override
   public ClusteredLock get(String name) {
      if (trace) {
         log.tracef("LOCK[%s] get has been called", name);
      }
      if (cache == null) {
         cache = extractCacheHolder(cacheHolderFuture).getClusteredLockCache();
      }

      return locks.computeIfAbsent(name, k -> {
         ClusteredLockKey key = new ClusteredLockKey(ByteString.fromString(k));
         if (!cache.containsKey(key)) {
            throw new ClusteredLockException(String.format("Lock does %s not exist", name));
         }
         return new ClusteredLockImpl(k, key, cache, this);
      });
   }

   @Override
   public ClusteredLockConfiguration getConfiguration(String name) {
      if (trace) {
         log.tracef("LOCK[%s] getConfiguration has been called", name);
      }
      CacheHolder cacheHolder = extractCacheHolder(cacheHolderFuture);
      cache = cacheHolder.getClusteredLockCache();
      if (cache.containsKey(new ClusteredLockKey(ByteString.fromString(name)))) {
         return new ClusteredLockConfiguration();
      }
      throw new ClusteredLockException(String.format("Lock does %s not exist", name));
   }

   @ManagedOperation(
         description = "Returns true if the lock is defined",
         displayName = "Is Lock Defined",
         name = IS_DEFINED
   )
   @Override
   public boolean isDefined(String name) {
      if (trace) {
         log.tracef("LOCK[%s] isDefined has been called", name);
      }
      if (cache == null) {
         cache = extractCacheHolder(cacheHolderFuture).getClusteredLockCache();
      }
      return cache.containsKey(new ClusteredLockKey(ByteString.fromString(name)));
   }

   @Override
   public CompletableFuture<Boolean> remove(String name) {
      if (trace) {
         log.tracef("LOCK[%s] remove has been called", name);
      }
      CacheHolder cacheHolder = extractCacheHolder(cacheHolderFuture);
      AdvancedCache<ClusteredLockKey, ClusteredLockValue> clusteredLockCache = cacheHolder.getClusteredLockCache();
      return clusteredLockCache.removeAsync(new ClusteredLockKey(ByteString.fromString(name))).thenApply(value -> {
         locks.remove(name);
         return value != null;
      });
   }

   @ManagedOperation(
         description = "Removes the lock from the cluster. The lock has to be recreated to access next time.",
         displayName = "Remove Clustered Lock",
         name = REMOVE
   )
   public boolean removeSync(String name) {
      if (trace) {
         log.tracef("LOCK[%s] remove sync has been called", name);
      }
      CacheHolder cacheHolder = extractCacheHolder(cacheHolderFuture);
      AdvancedCache<ClusteredLockKey, ClusteredLockValue> clusteredLockCache = cacheHolder.getClusteredLockCache();
      locks.remove(name);
      return clusteredLockCache.remove(new ClusteredLockKey(ByteString.fromString(name))) != null;
   }


   public CompletableFuture<Boolean> forceRelease(String name) {
      if (trace) {
         log.tracef("LOCK[%s] forceRelease has been called", name);
      }
      return CompletableFuture.supplyAsync(() -> forceReleaseSync(name));
   }

   @ManagedOperation(
         description = "Forces a release of the lock if such exist",
         displayName = "Release Clustered Lock",
         name = FORCE_RELEASE
   )
   public boolean forceReleaseSync(String name) {
      if (trace) {
         log.tracef("LOCK[%s] forceRelease sync has been called", name);
      }
      ClusteredLockValue clusteredLockValue = cache.computeIfPresent(new ClusteredLockKey(ByteString.fromString(name)), (k, v) -> ClusteredLockValue.INITIAL_STATE);
      return clusteredLockValue != null && clusteredLockValue.getState() == ClusteredLockState.RELEASED;
   }

   @ManagedOperation(
         description = "Returns true if the lock exists and is acquired",
         displayName = "Is Locked",
         name = IS_LOCKED
   )
   public boolean isLockedSync(String name) {
      if (trace) {
         log.tracef("LOCK[%s] isLocked sync has been called", name);
      }
      ClusteredLockValue clusteredLockValue = cache.get(new ClusteredLockKey(ByteString.fromString(name)));
      return clusteredLockValue != null && clusteredLockValue.getState() == ClusteredLockState.ACQUIRED;
   }

   private static CacheHolder extractCacheHolder(CompletableFuture<CacheHolder> future) {
      try {
         return future.get(WAIT_CACHES_TIMEOUT, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         log.fatal(e);
         throw new IllegalStateException("Clustered lock cache could not be started", e);
      } catch (ExecutionException | TimeoutException e) {
         log.fatal(e);
         throw new IllegalStateException("Clustered lock cache could not be started", e);
      }
   }

   public ScheduledExecutorService getScheduledExecutorService() {
      return scheduledExecutorService;
   }

   public void execute(Runnable runnable) {
      executor.execute(runnable);
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("EmbeddedClusteredLockManager{");
      sb.append(", address=").append(cache.getCacheManager().getAddress());
      sb.append(", locks=").append(locks);
      sb.append('}');
      return sb.toString();
   }
}
