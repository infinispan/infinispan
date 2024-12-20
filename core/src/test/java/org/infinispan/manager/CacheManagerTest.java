package org.infinispan.manager;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractGlobalComponentRegistry;
import static org.infinispan.test.TestingUtil.getDefaultCacheName;
import static org.infinispan.test.TestingUtil.getFirstStore;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.v;
import static org.infinispan.test.TestingUtil.waitForNoRebalance;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.infinispan.test.TestingUtil.withCacheManagers;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.junit.jupiter.api.Assertions.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.module.TestGlobalConfigurationBuilder;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.protostream.impl.GlobalMarshaller;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.dummy.Element;
import org.infinispan.persistence.spi.ExternalStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.TestException;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = {"functional", "smoke"}, testName = "manager.CacheManagerTest")
public class CacheManagerTest extends AbstractInfinispanTest {
   private static final java.lang.String CACHE_NAME = "name";

   public void testDefaultCache() {
      EmbeddedCacheManager cm = createCacheManager(false);

      try {
         assertEquals(ComponentStatus.RUNNING, cm.getCache().getStatus());
         assertEquals(getDefaultCacheName(cm), cm.getCache().getName());

         expectException(CacheConfigurationException.class,
                         () -> cm.defineConfiguration(getDefaultCacheName(cm),
                                                      new ConfigurationBuilder().build()));

         expectException(CacheConfigurationException.class, () -> cm.getCache("non-existent-cache"));
      } finally {
         killCacheManagers(cm);
      }
   }

   public void testDefaultCacheStartedAutomatically() {
      EmbeddedCacheManager cm = createCacheManager(new ConfigurationBuilder());
      try {
         assertEquals(new HashSet<>(Arrays.asList(getDefaultCacheName(cm))), cm.getCacheNames());

         ComponentRegistry cr = extractGlobalComponentRegistry(cm).getNamedComponentRegistry(getDefaultCacheName(cm));
         assertEquals(ComponentStatus.RUNNING, cr.getStatus());
         assertEquals(getDefaultCacheName(cm), cr.getCacheName());

      } finally {
         killCacheManagers(cm);
      }
   }

   public void testUnstartedCachemanager() {
      withCacheManager(new CacheManagerCallable(createCacheManager(false)) {
         @Override
         public void call() {
            assertEquals(ComponentStatus.INSTANTIATED, cm.getStatus());
            assertFalse(cm.getStatus().allowInvocations());
            Cache<Object, Object> cache = cm.getCache();
            cache.put("k", "v");
            assertEquals(cache.get("k"), "v");
         }
      });
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testClashingNames() {
      EmbeddedCacheManager cm = createCacheManager(false);
      try {
         ConfigurationBuilder c = new ConfigurationBuilder();
         cm.defineConfiguration("aCache", c.build());
         cm.defineConfiguration("aCache", c.build());
      } finally {
         killCacheManagers(cm);
      }
   }

   public void testWildcardTemplateNameMatchingInternalCache() {
      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      holder.newConfigurationBuilder("*")
            .template(true)
            .clustering().cacheMode(CacheMode.LOCAL)
            .memory().maxCount(1);
      EmbeddedCacheManager cm = createClusteredCacheManager(holder);
      try {
         Cache<Object, Object> aCache = cm.getCache("a");
         assertEquals(1L, aCache.getCacheConfiguration().memory().maxCount());
      } finally {
         killCacheManagers(cm);
      }
   }

   public void testStartAndStop() {
      EmbeddedCacheManager cm = createCacheManager(false);
      try {
         cm.defineConfiguration("cache1", new ConfigurationBuilder().build());
         cm.defineConfiguration("cache2", new ConfigurationBuilder().build());
         cm.defineConfiguration("cache3", new ConfigurationBuilder().build());
         Cache<?, ?> c1 = cm.getCache("cache1");
         Cache<?, ?> c2 = cm.getCache("cache2");
         Cache<?, ?> c3 = cm.getCache("cache3");

         assertEquals(ComponentStatus.RUNNING, c1.getStatus());
         assertEquals(ComponentStatus.RUNNING, c2.getStatus());
         assertEquals(ComponentStatus.RUNNING, c3.getStatus());

         cm.stop();

         assertEquals(ComponentStatus.TERMINATED, c1.getStatus());
         assertEquals(ComponentStatus.TERMINATED, c2.getStatus());
         assertEquals(ComponentStatus.TERMINATED, c3.getStatus());
      } finally {
         killCacheManagers(cm);
      }
   }

   public void testDefiningConfigurationValidation() {
      EmbeddedCacheManager cm = createCacheManager(false);

      expectException(NullPointerException.class,
                      () -> cm.defineConfiguration("cache1", null));

      expectException(NullPointerException.class,
                      () -> cm.defineConfiguration(null, null));

      expectException(NullPointerException.class,
                      () -> cm.defineConfiguration(null, new ConfigurationBuilder().build()));
   }

   public void testDefineConfigurationTwice() {
      EmbeddedCacheManager cm = createCacheManager(false);
      try {
         Configuration override = new ConfigurationBuilder().invocationBatching().enable().build();
         assertTrue(override.invocationBatching().enabled());
         assertTrue(cm.defineConfiguration("test1", override).invocationBatching().enabled());
         ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.read(override, Combine.DEFAULT);
         Configuration config = cb.build();
         assertTrue(config.invocationBatching().enabled());
         assertTrue(cm.defineConfiguration("test2", config).invocationBatching().enabled());
      } finally {
         cm.stop();
      }
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN000436:.*")
   public void testMissingDefaultConfiguration() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.jmx().enabled(false);
      EmbeddedCacheManager cm = new DefaultCacheManager(gcb.build());
      try {
         cm.getCache("someCache");
      } finally {
         cm.stop();
      }
   }

   public void testGetCacheNames() {
      EmbeddedCacheManager cm = createCacheManager(false);
      try {
         cm.defineConfiguration("one", new ConfigurationBuilder().build());
         cm.defineConfiguration("two", new ConfigurationBuilder().build());
         cm.defineConfiguration("three", new ConfigurationBuilder().build());
         cm.getCache("three");
         Set<String> cacheNames = cm.getCacheNames();
         assertEquals(4, cacheNames.size());
         assertTrue(cacheNames.contains("one"));
         assertTrue(cacheNames.contains("two"));
         assertTrue(cacheNames.contains("three"));
      } finally {
         cm.stop();
      }
   }

   public void testCacheStopTwice() {
      EmbeddedCacheManager localCacheManager = createCacheManager(false);
      try {
         Cache<String, String> cache = localCacheManager.getCache();
         cache.put("k", "v");
         cache.stop();
         cache.stop();
      } finally {
         killCacheManagers(localCacheManager);
      }
   }

   public void testCacheManagerStopTwice() {
      EmbeddedCacheManager localCacheManager = createCacheManager(false);
      try {
         Cache<String, String> cache = localCacheManager.getCache();
         cache.put("k", "v");
         localCacheManager.stop();
         localCacheManager.stop();
      } finally {
         killCacheManagers(localCacheManager);
      }
   }

   @Test(expectedExceptions = IllegalLifecycleStateException.class)
   public void testCacheStopManagerStopFollowedByGetCache() {
      EmbeddedCacheManager localCacheManager = createCacheManager(false);
      try {
         Cache<String, String> cache = localCacheManager.getCache();
         cache.put("k", "v");
         cache.stop();
         localCacheManager.stop();
         localCacheManager.getCache();
      } finally {
         killCacheManagers(localCacheManager);
      }
   }

   @Test(expectedExceptions = IllegalLifecycleStateException.class)
   public void testCacheStopManagerStopFollowedByCacheOp() {
      EmbeddedCacheManager localCacheManager = createCacheManager(false);
      try {
         Cache<String, String> cache = localCacheManager.getCache();
         cache.put("k", "v");
         cache.stop();
         localCacheManager.stop();
         cache.put("k", "v2");
      } finally {
         killCacheManagers(localCacheManager);
      }
   }

   public void testConcurrentCacheManagerStopAndGetCache() throws Exception {
      EmbeddedCacheManager manager = createCacheManager(false);
      CompletableFuture<Void> cacheStartBlocked = new CompletableFuture<>();
      CompletableFuture<Void> cacheStartResumed = new CompletableFuture<>();
      CompletableFuture<Void> managerStopBlocked = new CompletableFuture<>();
      CompletableFuture<Void> managerStopResumed = new CompletableFuture<>();
      try {
         manager.addListener(new MyListener(CACHE_NAME, cacheStartBlocked, cacheStartResumed));
         replaceComponent(manager, GlobalMarshaller.class, new LatchGlobalMarshaller(managerStopBlocked, managerStopResumed), true);
         // Need to start after replacing the global marshaller because we can't delegate to the old GlobalMarshaller
         manager.start();

         Future<?> cacheStartFuture = fork(() -> manager.createCache(CACHE_NAME, new ConfigurationBuilder().build()));
         cacheStartBlocked.get(10, SECONDS);

         Future<?> managerStopFuture = fork(() -> manager.stop());
         Exceptions.expectException(TimeoutException.class, () -> managerStopBlocked.get(1, SECONDS));

         Future<?> cacheStartFuture2 = fork(() -> manager.getCache(CACHE_NAME));
         Exceptions.expectExecutionException(IllegalLifecycleStateException.class, cacheStartFuture2);

         cacheStartResumed.complete(null);
         cacheStartFuture.get(10, SECONDS);

         managerStopBlocked.get(10, SECONDS);
         managerStopResumed.complete(null);
         managerStopFuture.get(10, SECONDS);
      } finally {
         cacheStartBlocked.complete(null);
         cacheStartResumed.complete(null);
         managerStopBlocked.complete(null);
         managerStopResumed.complete(null);

         killCacheManagers(manager);
      }
   }

   public void testConcurrentStopDuringStart() throws Exception {
      CheckPoint checkPoint = new CheckPoint();
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
      globalBuilder.addModule(TestGlobalConfigurationBuilder.class)
            .cacheManagerStartedCallback(() -> {
               checkPoint.trigger("cache_manager_starting");
               try {
                  checkPoint.awaitStrict("cache_manager_proceed", 15, SECONDS);
               } catch (InterruptedException | TimeoutException e) {
                  fail(e);
               }
            });
      EmbeddedCacheManager manager = createCacheManager(globalBuilder, new ConfigurationBuilder(), false);

      Future<Void> starting = fork(manager::start);
      checkPoint.awaitStrict("cache_manager_starting", 10, SECONDS);
      assertThat(manager.getStatus() == ComponentStatus.INITIALIZING).isTrue();

      // Stop while in INITIALIZING status.
      manager.stop();

      // Cache manager is able to TERMINATE.
      eventually(() -> manager.getStatus() == ComponentStatus.TERMINATED);
      checkPoint.trigger("cache_manager_proceed");

      // And start future does not throw.
      starting.get(10, SECONDS);

      // And after going to terminated, it doesn't come back.
      assertThat(manager.getStatus() == ComponentStatus.TERMINATED).isTrue();
   }

   public void testRemoveNonExistentCache(Method m) {
      EmbeddedCacheManager manager = getManagerWithStore(m, false, false);
      try {
         manager.getCache("cache");
         // An attempt to remove a non-existing cache should be a no-op
         manager.administration().removeCache("does-not-exist");
      } finally {
         manager.stop();
      }
   }

   public void testRemoveCacheLocal(Method m) {
      EmbeddedCacheManager manager = getManagerWithStore(m, false, false);
      try {
         Cache<String, String> cache = manager.getCache("cache");
         cache.put(k(m, 1), v(m, 1));
         cache.put(k(m, 2), v(m, 2));
         cache.put(k(m, 3), v(m, 3));
         DummyInMemoryStore store = getDummyStore(cache);
         DataContainer<?, ?> data = getDataContainer(cache);
         assertFalse(store.isEmpty());
         assertTrue(0 != data.size());
         manager.administration().removeCache("cache");
         assertEquals(0, DummyInMemoryStore.getStoreDataSize(store.getStoreName()));
         assertEquals(0, data.size());
         // Try removing the cache again, it should be a no-op
         manager.administration().removeCache("cache");
         assertEquals(0, DummyInMemoryStore.getStoreDataSize(store.getStoreName()));
         assertEquals(0, data.size());
      } finally {
         manager.stop();
      }
   }

   @Test(expectedExceptions = CacheException.class)
   public void testStartCachesFailed() {
      EmbeddedCacheManager cacheManager = null;
      try {
         GlobalConfigurationBuilder global = new GlobalConfigurationBuilder().nonClusteredDefault();
         TestCacheManagerFactory.addInterceptor(global, "incorrect"::equals, new ExceptionInterceptor(), TestCacheManagerFactory.InterceptorPosition.FIRST, null);
         cacheManager = createCacheManager(global, null);
         Configuration configuration = new ConfigurationBuilder().build();
         cacheManager.defineConfiguration("correct-cache-1", configuration);
         cacheManager.defineConfiguration("correct-cache-2", configuration);
         cacheManager.defineConfiguration("correct-cache-3", configuration);
         cacheManager.defineConfiguration("incorrect", configuration);
         cacheManager.startCaches("correct-cache-1", "correct-cache-2", "correct-cache-3", "incorrect");
      } finally {
         if (cacheManager != null) {
            killCacheManagers(cacheManager);
         }
      }
   }

   public void testCacheManagerStartFailure() {
      FailingGlobalComponent failingGlobalComponent = new FailingGlobalComponent();
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
      globalBuilder.addModule(TestGlobalConfigurationBuilder.class)
                   .testGlobalComponent(FailingGlobalComponent.class.getName(), failingGlobalComponent);
      ConfigurationBuilder builder = new ConfigurationBuilder();

      Exceptions.expectException(EmbeddedCacheManagerStartupException.class, () -> createCacheManager(globalBuilder, builder));

      assertTrue(failingGlobalComponent.started);
      assertTrue(failingGlobalComponent.stopped);
   }

   public void testCacheManagerRestartReusingConfigurations() {
      withCacheManagers(new MultiCacheManagerCallable(
            createCacheManager(CacheMode.REPL_SYNC, false),
            createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() {
            EmbeddedCacheManager cm1 = cms[0];
            EmbeddedCacheManager cm2 = cms[1];
            waitForNoRebalance(cm1.getCache(), cm2.getCache());
            Cache<Object, Object> c1 = cm1.getCache();

            GlobalConfiguration globalCfg = cm1.getCacheManagerConfiguration();
            Configuration cfg = c1.getCacheConfiguration();
            killCacheManagers(cm1);

            ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
            holder.getGlobalConfigurationBuilder().read(globalCfg);
            holder.getNamedConfigurationBuilders().put(TestCacheManagerFactory.DEFAULT_CACHE_NAME,
                                                       new ConfigurationBuilder().read(cfg, Combine.DEFAULT));

            withCacheManager(new CacheManagerCallable(new DefaultCacheManager(holder, true)) {
               @Override
               public void call() {
                  Cache<Object, Object> c = cm.getCache();
                  c.put(1, "v1");
                  assertEquals("v1", c.get(1));
               }
            });
         }
      });
   }

   public void testRemoveCacheClusteredLocalStores(Method m) throws Exception {
      doTestRemoveCacheClustered(m, false);
   }

   public void testRemoveCacheClusteredSharedStores(Method m) throws Exception {
      doTestRemoveCacheClustered(m, true);
   }

   public void testExceptionOnCacheManagerStop() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.persistence().addStore(UnreliableCacheStoreConfigurationBuilder.class)
            .segmented(false);
      EmbeddedCacheManager cm = createCacheManager(c);
      try {
         assertEquals(ComponentStatus.RUNNING, cm.getStatus());
         Cache<Integer, String> cache = cm.getCache();
         cache.put(1, "v1");
      } finally {
         killCacheManagers(cm);
         assertEquals(ComponentStatus.TERMINATED, cm.getStatus());
      }
   }

   public void testDefineConfigurationWithOverrideAndTemplate() {
      withCacheManager(new CacheManagerCallable(createClusteredCacheManager()) {
         @Override
         public void call() {
            // DIST_ASYNC isn't default so it should stay applied
            CacheMode cacheMode = CacheMode.DIST_ASYNC;
            String templateName = "dist-cache-template";
            cm.defineConfiguration(templateName, new ConfigurationBuilder().clustering().cacheMode(cacheMode).template(true).build());

            CacheMode overrideCacheMode = CacheMode.REPL_ASYNC;
            Configuration overrideConfiguration = new ConfigurationBuilder().clustering().cacheMode(overrideCacheMode).build();

            String ourCacheName = "my-cache";
            cm.defineConfiguration(ourCacheName, templateName, overrideConfiguration);

            Cache<?, ?> cache = cm.getCache(ourCacheName);
            // We expect the override to take precedence
            assertEquals(overrideCacheMode, cache.getCacheConfiguration().clustering().cacheMode());
         }
      });
   }

   public void testCacheNameLength() {
      final String cacheName = new String(new char[256]);
      final String exceptionMessage = String.format("ISPN000663: Name must be less than 256 bytes, current name '%s' exceeds the size.", cacheName);
      final Configuration configuration = new ConfigurationBuilder().build();

      withCacheManager(new CacheManagerCallable(createCacheManager()) {
         @Override
         public void call() {
            expectException(exceptionMessage, () -> cm.createCache(cacheName, configuration),
                  CacheConfigurationException.class);
            expectException(exceptionMessage, () -> cm.defineConfiguration(cacheName, configuration),
                  CacheConfigurationException.class);
         }
      });
   }

   private EmbeddedCacheManager getManagerWithStore(Method m, boolean isClustered, boolean isStoreShared) {
      return getManagerWithStore(m, isClustered, isStoreShared, "store-");
   }

   private EmbeddedCacheManager getManagerWithStore(Method m, boolean isClustered, boolean isStoreShared, String storePrefix) {
      String storeName = storePrefix + m.getName();
      ConfigurationBuilder c = new ConfigurationBuilder();
      c
            .persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class).storeName(storeName).shared(isStoreShared)
            .clustering()
               .cacheMode(isClustered ? CacheMode.REPL_SYNC : CacheMode.LOCAL);

      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().clusteredDefault();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(gcb, c);
      cm.defineConfiguration("cache", c.build());

      return cm;
   }

   private void doTestRemoveCacheClustered(final Method m, final boolean isStoreShared) {
      withCacheManagers(new MultiCacheManagerCallable(
            getManagerWithStore(m, true, isStoreShared, "store1-"),
            getManagerWithStore(m, true, isStoreShared, "store2-")) {
         @Override
         public void call() {
            EmbeddedCacheManager manager1 = cms[0];
            EmbeddedCacheManager manager2 = cms[0];

            Cache<String, String> cache1 = manager1.getCache("cache", true);
            Cache<String, String> cache2 = manager2.getCache("cache", true);
            assertNotNull(cache1);
            assertNotNull(cache2);
            assertTrue(manager1.cacheExists("cache"));
            assertTrue(manager2.cacheExists("cache"));
            cache1.put(k(m, 1), v(m, 1));
            cache1.put(k(m, 2), v(m, 2));
            cache1.put(k(m, 3), v(m, 3));
            cache2.put(k(m, 4), v(m, 4));
            cache2.put(k(m, 5), v(m, 5));
            DummyInMemoryStore store1 = getDummyStore(cache1);
            DataContainer<?, ?> data1 = getDataContainer(cache1);
            DummyInMemoryStore store2 = getDummyStore(cache2);
            DataContainer<?, ?> data2 = getDataContainer(cache2);
            assertFalse(store1.isEmpty());
            assertEquals(5, data1.size());
            assertFalse(store2.isEmpty());
            assertEquals(5, data2.size());

            manager1.administration().removeCache("cache");
            assertFalse(manager1.cacheExists("cache"));
            assertFalse(manager2.cacheExists("cache"));
            assertNull(manager1.getCache("cache", false));
            assertNull(manager2.getCache("cache", false));
            assertEquals(0, DummyInMemoryStore.getStoreDataSize(store1.getStoreName()));
            assertEquals(0, data1.size());
            assertEquals(0, DummyInMemoryStore.getStoreDataSize(store2.getStoreName()));
            assertEquals(0, data2.size());
         }
      });
   }

   private DummyInMemoryStore getDummyStore(Cache<String, String> cache1) {
      return (DummyInMemoryStore) getFirstStore(cache1);
   }

   private DataContainer<?, ?> getDataContainer(Cache<String, String> cache) {
      return extractComponent(cache, InternalDataContainer.class);
   }

   public static class UnreliableCacheStore implements ExternalStore<Object, Object> {
      @Override public void init(InitializationContext ctx) {}
      @Override public void write(MarshallableEntry<?, ?> entry) {}
      @Override public boolean delete(Object key) { return false; }
      @Override public MarshallableEntry<Object, Object> loadEntry(Object key) { return null; }
      @Override public boolean contains(Object key) { return false; }
      @Override public void start() {}
      @Override public void stop() {
         throw new IllegalStateException("Test");
      }
   }

   @ConfigurationFor(UnreliableCacheStore.class)
   @BuiltBy(UnreliableCacheStoreConfigurationBuilder.class)
   public static class UnreliableCacheStoreConfiguration extends AbstractStoreConfiguration<UnreliableCacheStoreConfiguration> {

      public UnreliableCacheStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async) {
         super(Element.DUMMY_STORE, attributes, async);
      }
   }

   public static class UnreliableCacheStoreConfigurationBuilder
         extends AbstractStoreConfigurationBuilder<UnreliableCacheStoreConfiguration, UnreliableCacheStoreConfigurationBuilder> {
      public UnreliableCacheStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) { super(builder, UnreliableCacheStoreConfiguration.attributeDefinitionSet()); }
      @Override public UnreliableCacheStoreConfiguration create() {
         return new UnreliableCacheStoreConfiguration(attributes.protect(), async.create());
      }
      @Override public UnreliableCacheStoreConfigurationBuilder self() { return this; }
   }

   static class ExceptionInterceptor extends BaseCustomAsyncInterceptor {
      @Override
      protected void start() {
         throw new IllegalStateException();
      }
   }

   @Listener
   private static class MyListener {
      private final String cacheName;
      private final CompletableFuture<Void> cacheStartBlocked;
      private final CompletableFuture<Void> cacheStartResumed;

      public MyListener(String cacheName, CompletableFuture<Void> cacheStartBlocked, CompletableFuture<Void> cacheStartResumed) {
         this.cacheName = cacheName;
         this.cacheStartBlocked = cacheStartBlocked;
         this.cacheStartResumed = cacheStartResumed;
      }

      @CacheStarted
      public void cacheStarted(CacheStartedEvent event) {
         log.tracef("Cache started: %s", event.getCacheName());
         if (cacheName.equals(event.getCacheName())) {
            cacheStartBlocked.complete(null);
            cacheStartResumed.join();
         }
      }

      @CacheStopped
      public void cacheStopped(CacheStoppedEvent event) {
         log.tracef("Cache stopped: %s", event.getCacheName());
      }
   }

   @Scope(Scopes.GLOBAL)
   public static class FailingGlobalComponent {
      private boolean started;
      private boolean stopped;

      @Start
      public void start() {
         started = true;
         throw new TestException();
      }

      @Stop
      public void stop() {
         stopped = true;
      }
   }

   static class LatchGlobalMarshaller extends GlobalMarshaller {
      private final CompletableFuture<Void> managerStopBlocked;
      private final CompletableFuture<Void> managerStopResumed;

      public LatchGlobalMarshaller(CompletableFuture<Void> managerStopBlocked,
                                   CompletableFuture<Void> managerStopResumed) {
         this.managerStopBlocked = managerStopBlocked;
         this.managerStopResumed = managerStopResumed;
      }

      @Override
      public void stop() {
         log.tracef("Stopping global component registry");
         managerStopBlocked.complete(null);
         managerStopResumed.join();
         super.stop();
      }
   }
}
