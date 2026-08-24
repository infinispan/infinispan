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
import java.util.function.Supplier;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.globalstate.impl.OverlayLocalConfigurationStorage;
import org.infinispan.interceptors.DDAsyncInterceptor;
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
   private static final String REPLY_HELD = "reply-held";
   private static final String RELEASE_REPLY = "release-reply";

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

   public void testUpdateCacheWhileJoinerReassertsCacheCreatedAfterStateTransfer(Method m) throws Exception {
      CheckPoint checkPoint = new CheckPoint();
      EmbeddedCacheManager cm1 = statefulManager(m, "1", null);
      EmbeddedCacheManager cm2 = statefulManager(m, "2",
            () -> new BlockOnReassertOverlayLocalConfigurationStorage(checkPoint, CACHE_NAME));
      try {
         cm1.start();
         // cm2 has the cache persisted locally, but not in CONFIG, so it re-asserts the cache.
         // Held after it found the cache absent from CONFIG, before the re-assert writes it.
         Future<Void> joinerStart = startAndHoldInStartupWindow(cm2, checkPoint);
         ScopedState key = new ScopedState(ScopeType.CACHE.toString(), CACHE_NAME);
         TestingUtil.extractInterceptorChain(TestingUtil.extractGlobalComponent(cm2, GlobalConfigurationManager.class).getStateCache())
               .addInterceptor(new HoldPutIfAbsentReplyInterceptor(checkPoint, key, testExecutor()), 0);

         // The cache reaches CONFIG after cm2 found it absent, so the re-assert returns the value created here (1000).
         fork(() -> cm1.administration().createCache(CACHE_NAME, cacheConfiguration(1000))).get(10, SECONDS);
         checkPoint.trigger(PROCEED);
         checkPoint.awaitStrict(REPLY_HELD, 10, SECONDS);

         // The update reaches cm2 before the re-assert defines the value it returned.
         fork(() -> cm1.administration()
               .withFlags(CacheContainerAdmin.AdminFlag.UPDATE)
               .getOrCreateCache(CACHE_NAME, cacheConfiguration(2000))).get(10, SECONDS);

         // The re-assert resumes with the value it returned before the update, which must not replace the update.
         checkPoint.trigger(RELEASE_REPLY);
         joinerStart.get(10, SECONDS);

         assertThat(cm1.getCacheConfiguration(CACHE_NAME).memory().maxCount()).isEqualTo(2000);
         assertThat(cm2.cacheExists(CACHE_NAME)).isTrue();
         assertThat(cm2.getCacheConfiguration(CACHE_NAME).memory().maxCount()).isEqualTo(2000);
      } finally {
         checkPoint.triggerForever(PROCEED);
         checkPoint.triggerForever(RELEASE_REPLY);
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
    * first time it validates the flags of a CONFIG write.
    * <p>
    * During startup, that is the re-assert of the persisted cache: after the node found the cache absent from its
    * CONFIG cache, and before the re-assert writes it.
    * </p>
    */
   static class BlockOnReassertOverlayLocalConfigurationStorage extends OverlayLocalConfigurationStorage {
      private final CheckPoint checkPoint;
      private final String cacheName;
      private final AtomicBoolean held = new AtomicBoolean();

      BlockOnReassertOverlayLocalConfigurationStorage(CheckPoint checkPoint, String cacheName) {
         this.checkPoint = checkPoint;
         this.cacheName = cacheName;
      }

      @Override
      public Map<String, Configuration> loadAllCaches() {
         Map<String, Configuration> caches = new HashMap<>(super.loadAllCaches());
         caches.put(cacheName, cacheConfiguration(1000));
         return caches;
      }

      @Override
      public void validateFlags(EnumSet<CacheContainerAdmin.AdminFlag> flags) {
         if (held.compareAndSet(false, true)) {
            checkPoint.trigger(HELD);
            try {
               checkPoint.awaitStrict(PROCEED, 10, SECONDS);
            } catch (Exception e) {
               throw new IllegalStateException("Startup was not released", e);
            }
         }
         super.validateFlags(flags);
      }
   }

   /**
    * Holds the reply of the node's own {@code putIfAbsent} for the given CONFIG key until {@link #RELEASE_REPLY} is
    * triggered, and signals {@link #REPLY_HELD} once the reply is held.
    * <p>
    * The write has already completed in the cluster when the reply is held, so the node receives the value the write
    * returned only after the release. The hold does not block a thread. Installed at the top of the chain, it only
    * matches the command on its originator.
    * </p>
    */
   static final class HoldPutIfAbsentReplyInterceptor extends DDAsyncInterceptor {
      private final CheckPoint checkPoint;
      private final ScopedState key;
      private final Executor executor;
      private final AtomicBoolean held = new AtomicBoolean();

      HoldPutIfAbsentReplyInterceptor(CheckPoint checkPoint, ScopedState key, Executor executor) {
         this.checkPoint = checkPoint;
         this.key = key;
         this.executor = executor;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
         if (!ctx.isOriginLocal() || !command.isPutIfAbsent() || !key.equals(command.getKey()) || !held.compareAndSet(false, true))
            return invokeNext(ctx, command);

         return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
            checkPoint.trigger(REPLY_HELD);
            return asyncValue(checkPoint.future(RELEASE_REPLY, 10, SECONDS, executor).thenApply(ignore -> rv));
         });
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
