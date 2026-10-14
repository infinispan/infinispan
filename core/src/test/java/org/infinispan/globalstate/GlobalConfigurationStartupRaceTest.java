package org.infinispan.globalstate;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.testing.Testing.tmpDirectory;

import java.lang.reflect.Method;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.impl.OverlayLocalConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * Verifies that cluster-wide configuration changes succeed while another node is still starting.
 * <p>
 * Changes made by other nodes while a node starts must neither fail on the starting node nor be lost by it. The tests
 * hold the joining node at different points of its startup by blocking its {@link LocalConfigurationStorage}: while it
 * loads the persisted caches, before it listens to the CONFIG cache, and, after it listens to the CONFIG cache, while
 * it defines the caches it received by CONFIG state transfer or re-asserts the caches it persisted.
 * </p>
 *
 * @since 16.3
 */
@Test(testName = "globalstate.GlobalConfigurationStartupRaceTest", groups = "functional")
public class GlobalConfigurationStartupRaceTest extends AbstractInfinispanTest {

   private static final String CACHE_NAME = "startup-race-cache";
   private static final String HELD = "held";
   private static final String PROCEED = "proceed";
   private static final String UPDATE_RECEIVED = "update-received";

   public void testCreateCacheWhileJoinerStarts(Method m) throws Exception {
      CheckPoint checkPoint = new CheckPoint();
      EmbeddedCacheManager cm1 = statefulManager(m, "1", null);
      EmbeddedCacheManager cm2 = statefulManager(m, "2", () -> new BlockingOverlayLocalConfigurationStorage(checkPoint));
      try {
         cm1.start();
         Future<Void> joinerStart = startAndHoldInStartupWindow(cm2, checkPoint);

         // cm2 is still loading its local state when the CONFIG write arrives.
         Future<?> create = fork(() -> cm1.administration().createCache(CACHE_NAME, cacheConfiguration(1000)));
         create.get(10, SECONDS);

         checkPoint.trigger(PROCEED);
         joinerStart.get(10, SECONDS);

         assertThat(cm2.cacheExists(CACHE_NAME)).isTrue();
         assertThat(cm2.getCacheConfiguration(CACHE_NAME).memory().maxCount()).isEqualTo(1000);
      } finally {
         checkPoint.triggerForever(PROCEED);
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testRemoveCacheWhileJoinerStarts(Method m) throws Exception {
      CheckPoint checkPoint = new CheckPoint();
      EmbeddedCacheManager cm1 = statefulManager(m, "1", null);
      EmbeddedCacheManager cm2 = statefulManager(m, "2", () -> new BlockingOverlayLocalConfigurationStorage(checkPoint));
      try {
         cm1.start();
         // The cache exists in CONFIG before cm2 joins, so cm2 receives it through state transfer.
         cm1.administration().createCache(CACHE_NAME, cacheConfiguration(1000));
         Future<Void> joinerStart = startAndHoldInStartupWindow(cm2, checkPoint);

         Future<?> remove = fork(() -> cm1.administration().removeCache(CACHE_NAME));
         remove.get(10, SECONDS);

         checkPoint.trigger(PROCEED);
         joinerStart.get(10, SECONDS);

         assertThat(cm1.cacheExists(CACHE_NAME)).isFalse();
         assertThat(cm2.cacheExists(CACHE_NAME)).isFalse();
         assertThat(cm2.getCacheConfiguration(CACHE_NAME)).isNull();
      } finally {
         checkPoint.triggerForever(PROCEED);
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testUpdateCacheWhileJoinerStarts(Method m) throws Exception {
      CheckPoint checkPoint = new CheckPoint();
      EmbeddedCacheManager cm1 = statefulManager(m, "1", null);
      EmbeddedCacheManager cm2 = statefulManager(m, "2", () -> new BlockingOverlayLocalConfigurationStorage(checkPoint));
      try {
         cm1.start();
         // cm2 receives the cache through CONFIG state transfer, but has not defined it locally while held.
         cm1.administration().createCache(CACHE_NAME, cacheConfiguration(1000));
         Future<Void> joinerStart = startAndHoldInStartupWindow(cm2, checkPoint);

         Future<?> update = fork(() -> cm1.administration()
               .withFlags(CacheContainerAdmin.AdminFlag.UPDATE)
               .getOrCreateCache(CACHE_NAME, cacheConfiguration(2000)));
         update.get(10, SECONDS);

         checkPoint.trigger(PROCEED);
         joinerStart.get(10, SECONDS);

         assertThat(cm1.getCacheConfiguration(CACHE_NAME).memory().maxCount()).isEqualTo(2000);
         assertThat(cm2.getCacheConfiguration(CACHE_NAME).memory().maxCount()).isEqualTo(2000);
      } finally {
         checkPoint.triggerForever(PROCEED);
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testUpdateCacheWhileJoinerDefinesExistingCaches(Method m) throws Exception {
      CheckPoint checkPoint = new CheckPoint();
      EmbeddedCacheManager cm1 = statefulManager(m, "1", null);
      EmbeddedCacheManager cm2 = statefulManager(m, "2",
            () -> new BlockOnFirstDefineOverlayLocalConfigurationStorage(checkPoint, CACHE_NAME));
      try {
         cm1.start();
         // cm2 receives the cache through CONFIG state transfer and defines it while starting.
         cm1.administration().createCache(CACHE_NAME, cacheConfiguration(1000));
         // Held after the listener is installed, while defining the cache from the value it read from CONFIG (1000).
         Future<Void> joinerStart = startAndHoldInStartupWindow(cm2, checkPoint);
         TestingUtil.extractGlobalComponent(cm2, GlobalConfigurationManager.class).getStateCache()
               .addListener(new UpdateProbe(checkPoint));

         Future<?> update = fork(() -> cm1.administration()
               .withFlags(CacheContainerAdmin.AdminFlag.UPDATE)
               .getOrCreateCache(CACHE_NAME, cacheConfiguration(2000)));
         // The update reached cm2 while it is still defining the value read before the update.
         checkPoint.awaitStrict(UPDATE_RECEIVED, 10, SECONDS);

         // The startup resumes with the value it read before the update, which must not replace the update.
         checkPoint.trigger(PROCEED);
         joinerStart.get(10, SECONDS);
         update.get(10, SECONDS);

         assertThat(cm1.getCacheConfiguration(CACHE_NAME).memory().maxCount()).isEqualTo(2000);
         assertThat(cm2.cacheExists(CACHE_NAME)).isTrue();
         assertThat(cm2.getCacheConfiguration(CACHE_NAME).memory().maxCount()).isEqualTo(2000);
      } finally {
         checkPoint.triggerForever(PROCEED);
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testUpdateCacheWhileJoinerReassertsPersistedCaches(Method m) throws Exception {
      CheckPoint checkPoint = new CheckPoint();
      EmbeddedCacheManager cm1 = statefulManager(m, "1", null);
      EmbeddedCacheManager cm2 = statefulManager(m, "2",
            () -> new BlockOnReassertOverlayLocalConfigurationStorage(checkPoint, CACHE_NAME, testExecutor()));
      try {
         cm1.start();
         // cm2 receives the cache through CONFIG state transfer and also has it persisted locally.
         cm1.administration().createCache(CACHE_NAME, cacheConfiguration(1000));
         // Held after the listener is installed, while re-asserting the persisted cache with the value its
         // putIfAbsent returned from CONFIG (1000).
         Future<Void> joinerStart = startAndHoldInStartupWindow(cm2, checkPoint);
         TestingUtil.extractGlobalComponent(cm2, GlobalConfigurationManager.class).getStateCache()
               .addListener(new UpdateProbe(checkPoint));

         Future<?> update = fork(() -> cm1.administration()
               .withFlags(CacheContainerAdmin.AdminFlag.UPDATE)
               .getOrCreateCache(CACHE_NAME, cacheConfiguration(2000)));
         // The update reached cm2 while it is still defining the value returned before the update.
         checkPoint.awaitStrict(UPDATE_RECEIVED, 10, SECONDS);

         // The re-assert resumes with the value it read before the update, which must not replace the update.
         checkPoint.trigger(PROCEED);
         joinerStart.get(10, SECONDS);
         update.get(10, SECONDS);

         assertThat(cm1.getCacheConfiguration(CACHE_NAME).memory().maxCount()).isEqualTo(2000);
         assertThat(cm2.cacheExists(CACHE_NAME)).isTrue();
         assertThat(cm2.getCacheConfiguration(CACHE_NAME).memory().maxCount()).isEqualTo(2000);
      } finally {
         checkPoint.triggerForever(PROCEED);
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   /**
    * Starts the cache manager in the background and returns once its configuration storage holds the startup.
    */
   private Future<Void> startAndHoldInStartupWindow(EmbeddedCacheManager cm, CheckPoint checkPoint) throws Exception {
      Future<Void> start = fork(cm::start);
      checkPoint.awaitStrict(HELD, 10, SECONDS);
      return start;
   }

   /**
    * Creates a cache manager with global state enabled. It uses overlay configuration storage when {@code storage} is
    * {@code null}, and the given storage otherwise.
    */
   private EmbeddedCacheManager statefulManager(Method m, String suffix, Supplier<? extends LocalConfigurationStorage> storage) {
      String stateDirectory = tmpDirectory(getClass().getSimpleName(), m.getName() + suffix);
      Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable()
            .persistentLocation(stateDirectory)
            .sharedPersistentLocation(stateDirectory);
      if (storage == null) {
         global.globalState().configurationStorage(ConfigurationStorage.OVERLAY);
      } else {
         global.globalState()
               .configurationStorage(ConfigurationStorage.CUSTOM)
               .configurationStorageSupplier(storage);
      }
      return TestCacheManagerFactory.createClusteredCacheManager(false, global, new ConfigurationBuilder(), new TransportFlags());
   }

   private static Configuration cacheConfiguration(long maxCount) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().maxCount(maxCount);
      return builder.build();
   }

   /**
    * Overlay storage that pauses the node's startup while it loads the persisted caches.
    */
   static class BlockingOverlayLocalConfigurationStorage extends OverlayLocalConfigurationStorage {
      private final CheckPoint checkPoint;

      BlockingOverlayLocalConfigurationStorage(CheckPoint checkPoint) {
         this.checkPoint = checkPoint;
      }

      @Override
      public Map<String, Configuration> loadAllCaches() {
         checkPoint.trigger(HELD);
         try {
            checkPoint.awaitStrict(PROCEED, 10, SECONDS);
         } catch (Exception e) {
            throw new IllegalStateException("Startup was not released", e);
         }
         return super.loadAllCaches();
      }
   }

   /**
    * Overlay storage that pauses the node's startup the first time it defines the given cache. During startup, that is
    * the definition of an entry the node received by CONFIG state transfer, after the CONFIG listener is installed.
    * Later definitions, such as the ones made by the listener, are not paused.
    */
   static class BlockOnFirstDefineOverlayLocalConfigurationStorage extends OverlayLocalConfigurationStorage {
      private final CheckPoint checkPoint;
      private final String cacheName;
      private final AtomicBoolean held = new AtomicBoolean();

      BlockOnFirstDefineOverlayLocalConfigurationStorage(CheckPoint checkPoint, String cacheName) {
         this.checkPoint = checkPoint;
         this.cacheName = cacheName;
      }

      @Override
      public CompletionStage<Void> defineCacheConfiguration(String name, String template, Configuration configuration,
                                                            EnumSet<CacheContainerAdmin.AdminFlag> flags) {
         if (cacheName.equals(name) && held.compareAndSet(false, true)) {
            checkPoint.trigger(HELD);
            try {
               checkPoint.awaitStrict(PROCEED, 10, SECONDS);
            } catch (Exception e) {
               throw new IllegalStateException("Startup was not released", e);
            }
         }
         return super.defineCacheConfiguration(name, template, configuration, flags);
      }
   }

   /**
    * Overlay storage that reports the given cache as persisted by a previous run, and pauses the node's startup the
    * second time it defines that cache.
    * <p>
    * During startup, the first definition comes from the entry received by CONFIG state transfer. The second one is the
    * re-assert of the persisted cache, defining the value returned by its {@code putIfAbsent} on the CONFIG cache. The
    * re-assert might run on a non-blocking thread, so the pause does not block the thread.
    */
   static class BlockOnReassertOverlayLocalConfigurationStorage extends OverlayLocalConfigurationStorage {
      private final CheckPoint checkPoint;
      private final String cacheName;
      private final Executor executor;
      private final AtomicInteger defines = new AtomicInteger();

      BlockOnReassertOverlayLocalConfigurationStorage(CheckPoint checkPoint, String cacheName, Executor executor) {
         this.checkPoint = checkPoint;
         this.cacheName = cacheName;
         this.executor = executor;
      }

      @Override
      public Map<String, Configuration> loadAllCaches() {
         Map<String, Configuration> caches = new HashMap<>(super.loadAllCaches());
         caches.put(cacheName, cacheConfiguration(1000));
         return caches;
      }

      @Override
      public CompletionStage<Void> defineCacheConfiguration(String name, String template, Configuration configuration,
                                                            EnumSet<CacheContainerAdmin.AdminFlag> flags) {
         if (cacheName.equals(name) && defines.incrementAndGet() == 2) {
            checkPoint.trigger(HELD);
            return checkPoint.future(PROCEED, 10, SECONDS, executor)
                  .thenCompose(ignore -> super.defineCacheConfiguration(name, template, configuration, flags));
         }
         return super.defineCacheConfiguration(name, template, configuration, flags);
      }
   }

   /**
    * Signals when the CONFIG cache of the node it is registered on receives an update for {@link #CACHE_NAME}.
    */
   @Listener
   public static class UpdateProbe {
      private final CheckPoint checkPoint;

      UpdateProbe(CheckPoint checkPoint) {
         this.checkPoint = checkPoint;
      }

      @CacheEntryModified
      public void onUpdate(CacheEntryModifiedEvent<ScopedState, Object> event) {
         if (!event.isPre() && CACHE_NAME.equals(event.getKey().getName()))
            checkPoint.trigger(UPDATE_RECEIVED);
      }
   }
}
