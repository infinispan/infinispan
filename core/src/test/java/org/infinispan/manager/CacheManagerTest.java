package org.infinispan.manager;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.test.TestingUtil.v;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.infinispan.test.TestingUtil.withCacheManagers;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.core.GlobalMarshaller;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.ExternalStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.TestingUtil;
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
         assertTrue(cm.getCache().getName().equals(CacheContainer.DEFAULT_CACHE_NAME));

         expectException(IllegalArgumentException.class,
                         () -> cm.defineConfiguration(CacheContainer.DEFAULT_CACHE_NAME,
                                                      new ConfigurationBuilder().build()));
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testUnstartedCachemanager() {
      withCacheManager(new CacheManagerCallable(createCacheManager(false)){
         @Override
         public void call() {
            assertTrue(cm.getStatus().equals(ComponentStatus.INSTANTIATED));
            assertFalse(cm.getStatus().allowInvocations());
            Cache<Object, Object> cache = cm.getCache();
            cache.put("k","v");
            assertTrue(cache.get("k").equals("v"));
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
         TestingUtil.killCacheManagers(cm);
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
         TestingUtil.killCacheManagers(cm);
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

   public void testDefiningConfigurationOverridingBooleans() {
      EmbeddedCacheManager cm = createCacheManager(false);
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.memory().storageType(StorageType.BINARY);
      Configuration lazy = cm.defineConfiguration("storeAsBinary", c.build());
      assertEquals(StorageType.BINARY, lazy.memory().storageType());

      c = new ConfigurationBuilder().read(lazy);
      c.memory().storageType(StorageType.OFF_HEAP).size(1);
      Configuration lazyOffHeap = cm.defineConfiguration("lazyDeserializationWithOffHeap", c.build());
      assertEquals(StorageType.OFF_HEAP, lazyOffHeap.memory().storageType());
      assertEquals(1, lazyOffHeap.memory().size());
   }

   public void testDefineConfigurationTwice() {
      EmbeddedCacheManager cm = createCacheManager(false);
      try {
         Configuration override = new ConfigurationBuilder().invocationBatching().enable().build();
         assertTrue(override.invocationBatching().enabled());
         assertTrue(cm.defineConfiguration("test1", override).invocationBatching().enabled());
         ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.read(override);
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
      gcb.globalJmxStatistics().jmxDomain("infinispan-" + UUID.randomUUID());
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
         assertEquals(3, cacheNames.size());
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
         TestingUtil.killCacheManagers(localCacheManager);
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
         TestingUtil.killCacheManagers(localCacheManager);
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
         TestingUtil.killCacheManagers(localCacheManager);
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
         TestingUtil.killCacheManagers(localCacheManager);
      }
   }

   public void testConcurrentCacheManagerStopAndGetCache() throws Exception {
      EmbeddedCacheManager manager = createCacheManager(false);
      try {
         CompletableFuture<Void> cacheStartBlocked = new CompletableFuture<>();
         CompletableFuture<Void> cacheStartResumed = new CompletableFuture<>();
         CompletableFuture<Void> managerStopBlocked = new CompletableFuture<>();
         CompletableFuture<Void> managerStopResumed = new CompletableFuture<>();
         manager.addListener(new MyListener(cacheStartBlocked, cacheStartResumed));
         TestingUtil.replaceComponent(manager, GlobalMarshaller.class, new GlobalMarshaller() {
            @Override
            public void stop() {
               log.tracef("Stopping global component registry");
               managerStopBlocked.complete(null);
               managerStopResumed.join();
               super.stop();
            }
         }, true);

         Future<?> cacheStartFuture = fork(() -> manager.getCache(CACHE_NAME));
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
         TestingUtil.killCacheManagers(manager);
      }
   }

   public void testRemoveNonExistentCache(Method m) {
      EmbeddedCacheManager manager = getManagerWithStore(m, false, false);
      try {
         manager.getCache("cache");
         // An attempt to remove a non-existing cache should be a no-op
         manager.removeCache("does-not-exist");
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
         DataContainer data = getDataContainer(cache);
         assertFalse(store.isEmpty());
         assertTrue(0 != data.size());
         manager.removeCache("cache");
         assertEquals(0, DummyInMemoryStore.getStoreDataSize(store.getStoreName()));
         assertEquals(0, data.size());
         // Try removing the cache again, it should be a no-op
         manager.removeCache("cache");
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
         cacheManager = createCacheManager();
         cacheManager.defineConfiguration("correct-cache-1", cacheManager.getDefaultCacheConfiguration());
         cacheManager.defineConfiguration("correct-cache-2", cacheManager.getDefaultCacheConfiguration());
         cacheManager.defineConfiguration("correct-cache-3", cacheManager.getDefaultCacheConfiguration());
         ConfigurationBuilder incorrectBuilder = new ConfigurationBuilder();
         incorrectBuilder.customInterceptors().addInterceptor().position(InterceptorConfiguration.Position.FIRST)
               .interceptor(new BaseCustomInterceptor() {
                  @Override
                  protected void start() {
                     throw new IllegalStateException();
                  }
               });
         cacheManager.defineConfiguration("incorrect", incorrectBuilder.build());
         cacheManager.startCaches("correct-cache-1", "correct-cache-2", "correct-cache-3", "incorrect");
      } finally {
         if (cacheManager != null) {
            killCacheManagers(cacheManager);
         }
      }
   }

   public void testCacheManagerRestartReusingConfigurations() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() {
            EmbeddedCacheManager cm1 = cms[0];
            EmbeddedCacheManager cm2 = cms[1];
            Cache<Object, Object> c1 = cm1.getCache();
            cm2.getCache();

            GlobalConfiguration globalCfg = cm1.getCacheManagerConfiguration();
            Configuration cfg = c1.getCacheConfiguration();
            cm1.stop();

            withCacheManager(new CacheManagerCallable(
                  new DefaultCacheManager(globalCfg, cfg)) {
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
      c.persistence().addStore(UnreliableCacheStoreConfigurationBuilder.class);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(c);
      try {
         assertEquals(ComponentStatus.RUNNING, cm.getStatus());
         Cache<Integer, String> cache = cm.getCache();
         cache.put(1, "v1");
      } finally {
         killCacheManagers(cm);
         assertEquals(ComponentStatus.TERMINATED, cm.getStatus());
      }
   }

   public void testGetCacheWithTemplateAlreadyDefinedConfiguration() {
      withCacheManager(new CacheManagerCallable(createClusteredCacheManager()) {
         @Override
         public void call() {
            ConfigurationBuilder builder = new ConfigurationBuilder();
            CacheMode cacheMode = CacheMode.DIST_ASYNC;
            // DIST_ASYNC isn't default so it should stay applied
            builder.clustering().cacheMode(cacheMode);
            String distCacheName = "dist-cache";
            cm.defineConfiguration(distCacheName, builder.build());

            String templateName = "template";
            cm.defineConfiguration(templateName, new ConfigurationBuilder().template(true).build());

            Cache cache = cm.getCache(distCacheName, templateName);
            assertEquals(cacheMode, cache.getCacheConfiguration().clustering().cacheMode());
         }
      });
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

            Cache cache = cm.getCache(ourCacheName);
            // We expect the override to take precedence
            assertEquals(overrideCacheMode, cache.getCacheConfiguration().clustering().cacheMode());
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
            assertTrue(cache1 != null);
            assertTrue(cache2 != null);
            assertTrue(manager1.cacheExists("cache"));
            assertTrue(manager2.cacheExists("cache"));
            cache1.put(k(m, 1), v(m, 1));
            cache1.put(k(m, 2), v(m, 2));
            cache1.put(k(m, 3), v(m, 3));
            cache2.put(k(m, 4), v(m, 4));
            cache2.put(k(m, 5), v(m, 5));
            DummyInMemoryStore store1 = getDummyStore(cache1);
            DataContainer data1 = getDataContainer(cache1);
            DummyInMemoryStore store2 = getDummyStore(cache2);
            DataContainer data2 = getDataContainer(cache2);
            assertFalse(store1.isEmpty());
            assertEquals(5, data1.size());
            assertFalse(store2.isEmpty());
            assertEquals(5, data2.size());

            manager1.removeCache("cache");
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
      return (DummyInMemoryStore) TestingUtil.getFirstLoader(cache1);
   }

   private DataContainer getDataContainer(Cache<String, String> cache) {
      return TestingUtil.extractComponent(cache, DataContainer.class);
   }

   public static class UnreliableCacheStore implements ExternalStore {
      @Override public void init(InitializationContext ctx) {}
      @Override public void write(MarshalledEntry entry) {}
      @Override public boolean delete(Object key) { return false; }
      @Override public MarshalledEntry load(Object key) { return null; }
      @Override public boolean contains(Object key) { return false; }
      @Override public void start() {}
      @Override public void stop() {
         throw new IllegalStateException("Test");
      }
   }

   @ConfigurationFor(UnreliableCacheStore.class)
   @BuiltBy(UnreliableCacheStoreConfigurationBuilder.class)
   public static class UnreliableCacheStoreConfiguration extends AbstractStoreConfiguration {

      public UnreliableCacheStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
         super(attributes, async, singletonStore);
      }
   }

   public static class UnreliableCacheStoreConfigurationBuilder
         extends AbstractStoreConfigurationBuilder<UnreliableCacheStoreConfiguration, UnreliableCacheStoreConfigurationBuilder> {
      public UnreliableCacheStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) { super(builder, UnreliableCacheStoreConfiguration.attributeDefinitionSet()); }
      @Override public UnreliableCacheStoreConfiguration create() {
         return new UnreliableCacheStoreConfiguration(attributes.protect(), async.create(), singleton().create());
      }
      @Override public UnreliableCacheStoreConfigurationBuilder self() { return this; }
   }

   @Listener
   private class MyListener {
      private final CompletableFuture<Void> cacheStartBlocked;
      private final CompletableFuture<Void> cacheStartResumed;

      public MyListener(CompletableFuture<Void> cacheStartBlocked, CompletableFuture<Void> cacheStartResumed) {
         this.cacheStartBlocked = cacheStartBlocked;
         this.cacheStartResumed = cacheStartResumed;
      }

      @CacheStarted
      public void cacheStarted(CacheStartedEvent event) {
         log.tracef("Cache started: %s", event.getCacheName());
         cacheStartBlocked.complete(null);
         cacheStartResumed.join();
      }

      @CacheStopped
      public void cacheStopped(CacheStoppedEvent event) {
         log.tracef("Cache stopped: %s", event.getCacheName());
      }
   }
}
