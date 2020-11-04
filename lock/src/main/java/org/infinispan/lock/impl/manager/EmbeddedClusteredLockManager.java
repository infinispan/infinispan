package org.infinispan.lock.impl.manager;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.context.Flag;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockConfiguration;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.configuration.ClusteredLockManagerConfiguration;
import org.infinispan.lock.exception.ClusteredLockException;
import org.infinispan.lock.impl.ClusteredLockModuleLifecycle;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockState;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.impl.lock.ClusteredLockImpl;
import org.infinispan.lock.logging.Log;
import org.infinispan.manager.EmbeddedCacheManager;
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
   private static final Log log = LogFactory.getLog(EmbeddedClusteredLockManager.class, Log.class);
   private final boolean trace = log.isTraceEnabled();
   public static final String FORCE_RELEASE = "forceRelease";
   public static final String REMOVE = "remove";
   public static final String IS_DEFINED = "isDefined";
   public static final String IS_LOCKED = "isLocked";

   private final ConcurrentHashMap<String, ClusteredLock> locks = new ConcurrentHashMap<>();
   private final ClusteredLockManagerConfiguration config;
   private volatile boolean started = false;

   @Inject
   EmbeddedCacheManager cacheManager;

   @Inject @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   ScheduledExecutorService scheduledExecutorService;

   private AdvancedCache<ClusteredLockKey, ClusteredLockValue> cache;

   public EmbeddedClusteredLockManager(ClusteredLockManagerConfiguration config) {
      this.config = config;
   }

   @Start
   public void start() {
      if (trace)
         log.trace("Starting EmbeddedClusteredLockManager");

      started = true;
   }

   @Stop
   public void stop() {
      if (trace)
         log.trace("Stopping EmbeddedClusteredLockManager");

      started = false;
      cache = null;
   }

   private AdvancedCache<ClusteredLockKey, ClusteredLockValue> cache() {
      if (!started) {
        throw new IllegalStateException("Component not running, cannot request the lock cache");
      } else if (cache == null) {
         cache = cacheManager.<ClusteredLockKey, ClusteredLockValue>getCache(ClusteredLockModuleLifecycle.CLUSTERED_LOCK_CACHE_NAME)
               .getAdvancedCache()
               .withFlags(Flag.SKIP_CACHE_LOAD, Flag.SKIP_CACHE_STORE);
      }
      return cache;
   }

   @Override
   public boolean defineLock(String name) {
      ClusteredLockConfiguration configuration = new ClusteredLockConfiguration();
      if (trace)
         log.tracef("LOCK[%s] defineLock with default configuration has been called %s", name, configuration);

      return defineLock(name, configuration);
   }

   @Override
   public boolean defineLock(String name, ClusteredLockConfiguration configuration) {
      if (trace)
         log.tracef("LOCK[%s] defineLock has been called %s", name, configuration);

      ClusteredLockKey key = new ClusteredLockKey(ByteString.fromString(name));
      ClusteredLockValue clv = cache().putIfAbsent(key, ClusteredLockValue.INITIAL_STATE);
      locks.putIfAbsent(name, new ClusteredLockImpl(name, key, cache, this));
      return clv == null;
   }

   @Override
   public ClusteredLock get(String name) {
      if (trace)
         log.tracef("LOCK[%s] get has been called", name);

      return locks.computeIfAbsent(name, this::createLock);
   }

   private ClusteredLockImpl createLock(String lockName) {
      ClusteredLockConfiguration configuration = getConfiguration(lockName);
      if (configuration == null) {
         throw new ClusteredLockException(String.format("Lock %s does not exist", lockName));
      }
      ClusteredLockKey key = new ClusteredLockKey(ByteString.fromString(lockName));
      cache().putIfAbsent(key, ClusteredLockValue.INITIAL_STATE);
      ClusteredLockImpl lock = new ClusteredLockImpl(lockName, key, cache(), this);
      return lock;
   }

   @Override
   public ClusteredLockConfiguration getConfiguration(String name) {
      if (trace)
         log.tracef("LOCK[%s] getConfiguration has been called", name);

      if (cache().containsKey(new ClusteredLockKey(ByteString.fromString(name))))
         return new ClusteredLockConfiguration();

      if (config.locks().containsKey(name))
         return new ClusteredLockConfiguration();

      throw new ClusteredLockException(String.format("Lock %s does not exist", name));
   }

   @ManagedOperation(
         description = "Returns true if the lock is defined",
         displayName = "Is Lock Defined",
         name = IS_DEFINED
   )
   @Override
   public boolean isDefined(String name) {
      if (trace)
         log.tracef("LOCK[%s] isDefined has been called", name);

      return cache().containsKey(new ClusteredLockKey(ByteString.fromString(name)));
   }

   @Override
   public CompletableFuture<Boolean> remove(String name) {
      if (trace)
         log.tracef("LOCK[%s] remove has been called", name);

      ClusteredLockImpl clusteredLock = (ClusteredLockImpl) locks.get(name);
      if (clusteredLock != null) {
         clusteredLock.stop();
         locks.remove(name);
      }

      return cache()
            .removeAsync(new ClusteredLockKey(ByteString.fromString(name)))
            .thenApply(Objects::nonNull);
   }

   @ManagedOperation(
         description = "Removes the lock from the cluster. The lock has to be recreated to access next time.",
         displayName = "Remove Clustered Lock",
         name = REMOVE
   )
   public boolean removeSync(String name) {
      if (trace)
         log.tracef("LOCK[%s] remove sync has been called", name);

      ClusteredLockImpl clusteredLock = (ClusteredLockImpl) locks.get(name);
      if (clusteredLock != null) {
         clusteredLock.stop();
         locks.remove(name);
      }

      return cache().remove(new ClusteredLockKey(ByteString.fromString(name))) != null;
   }

   public CompletableFuture<Boolean> forceRelease(String name) {
      if (trace)
         log.tracef("LOCK[%s] forceRelease has been called", name);

      ClusteredLockKey lockLey = new ClusteredLockKey(ByteString.fromString(name));
      return cache()
            .computeIfPresentAsync(lockLey, (k, v) -> ClusteredLockValue.INITIAL_STATE)
            .thenApply(clv -> clv != null && clv.getState() == ClusteredLockState.RELEASED);
   }

   @ManagedOperation(
         description = "Forces a release of the lock if such exist",
         displayName = "Release Clustered Lock",
         name = FORCE_RELEASE
   )
   public boolean forceReleaseSync(String name) {
      if (trace)
         log.tracef("LOCK[%s] forceRelease sync has been called", name);

      return forceRelease(name).join();
   }

   @ManagedOperation(
         description = "Returns true if the lock exists and is acquired",
         displayName = "Is Locked",
         name = IS_LOCKED
   )
   public boolean isLockedSync(String name) {
      if (trace)
         log.tracef("LOCK[%s] isLocked sync has been called", name);

      ClusteredLockValue clv = cache().get(new ClusteredLockKey(ByteString.fromString(name)));
      return clv != null && clv.getState() == ClusteredLockState.ACQUIRED;
   }

   public ScheduledExecutorService getScheduledExecutorService() {
      return scheduledExecutorService;
   }

   @Override
   public String toString() {
      return "EmbeddedClusteredLockManager{" +
            ", address=" + cacheManager.getAddress() +
            ", locks=" + locks +
            '}';
   }
}
