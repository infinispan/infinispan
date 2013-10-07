package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Set;

import static org.infinispan.test.TestingUtil.*;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "manager.CacheManagerTest")
public class CacheManagerTest extends AbstractInfinispanTest {
   public void testDefaultCache() {
      EmbeddedCacheManager cm = createCacheManager(false);

      try {
         assert cm.getCache().getStatus() == ComponentStatus.RUNNING;
         assert cm.getCache().getName().equals(CacheContainer.DEFAULT_CACHE_NAME);

         try {
            cm.defineConfiguration(CacheContainer.DEFAULT_CACHE_NAME, new ConfigurationBuilder().build());
            assert false : "Should fail";
         }
         catch (IllegalArgumentException e) {
            assert true; // ok
         }
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testUnstartedCachemanager() {
      withCacheManager(new CacheManagerCallable(createCacheManager(false)){
         @Override
         public void call() {
            assert cm.getStatus().equals(ComponentStatus.INSTANTIATED);
            assert !cm.getStatus().allowInvocations();
            Cache<Object, Object> cache = cm.getCache();
            cache.put("k","v");
            assert cache.get("k").equals("v");
         }
      });
   }

   public void testClashingNames() {
      EmbeddedCacheManager cm = createCacheManager(false);
      try {
         ConfigurationBuilder c = new ConfigurationBuilder();
         Configuration firstDef = cm.defineConfiguration("aCache", c.build());
         Configuration secondDef = cm.defineConfiguration("aCache", c.build());
         assert firstDef.equals(secondDef);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testStartAndStop() {
      CacheContainer cm = createCacheManager(false);
      try {
         Cache<?, ?> c1 = cm.getCache("cache1");
         Cache<?, ?> c2 = cm.getCache("cache2");
         Cache<?, ?> c3 = cm.getCache("cache3");

         assert c1.getStatus() == ComponentStatus.RUNNING;
         assert c2.getStatus() == ComponentStatus.RUNNING;
         assert c3.getStatus() == ComponentStatus.RUNNING;

         cm.stop();

         assert c1.getStatus() == ComponentStatus.TERMINATED;
         assert c2.getStatus() == ComponentStatus.TERMINATED;
         assert c3.getStatus() == ComponentStatus.TERMINATED;
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testDefiningConfigurationValidation() {
      EmbeddedCacheManager cm = createCacheManager(false);
      try {
         cm.defineConfiguration("cache1", (Configuration) null);
         assert false : "Should fail";
      } catch(NullPointerException npe) {
         assert npe.getMessage() != null;
      }

      try {
         cm.defineConfiguration(null, (Configuration) null);
         assert false : "Should fail";
      } catch(NullPointerException npe) {
         assert npe.getMessage() != null;
      }

      try {
         cm.defineConfiguration(null, new ConfigurationBuilder().build());
         assert false : "Should fail";
      } catch(NullPointerException npe) {
         assert npe.getMessage() != null;
      }
   }

   public void testDefiningConfigurationOverridingBooleans() {
      EmbeddedCacheManager cm = createCacheManager(false);
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.storeAsBinary().enable();
      Configuration lazy = cm.defineConfiguration("storeAsBinary", c.build());
      assert lazy.storeAsBinary().enabled();

      c = new ConfigurationBuilder().read(lazy);
      c.eviction().strategy(EvictionStrategy.LRU).maxEntries(1);
      Configuration lazyLru = cm.defineConfiguration("lazyDeserializationWithLRU", c.build());
      assert lazy.storeAsBinary().enabled();
      assert lazyLru.eviction().strategy() == EvictionStrategy.LRU;
   }

   public void testDefineConfigurationTwice() {
      EmbeddedCacheManager cm = createCacheManager(false);
      try {
         Configuration override = new ConfigurationBuilder().invocationBatching().enable().build();
         assert override.invocationBatching().enabled();
         assert cm.defineConfiguration("test1", override).invocationBatching().enabled();
         ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.read(override);
         Configuration config = cb.build();
         assert config.invocationBatching().enabled();
         assert cm.defineConfiguration("test2", config).invocationBatching().enabled();
      } finally {
         cm.stop();
      }
   }

   public void testGetCacheConfigurationAfterDefiningSameOldConfigurationTwice() {
      withCacheManager(new CacheManagerCallable(createCacheManager(false)) {
         @Override
         public void call() {
            ConfigurationBuilder c = new ConfigurationBuilder();
            c.invocationBatching().enable(false);
            Configuration newConfig = cm.defineConfiguration("new-cache", c.build());
            assert !newConfig.invocationBatching().enabled();

            c = new ConfigurationBuilder();
            c.invocationBatching().enable();
            Configuration newConfig2 = cm.defineConfiguration("new-cache", c.build());
            assert newConfig2.invocationBatching().enabled();
            assert cm.getCache("new-cache").getCacheConfiguration().invocationBatching().enabled();
         }
      });
   }

   public void testGetCacheConfigurationAfterDefiningSameNewConfigurationTwice() {
      withCacheManager(new CacheManagerCallable(createCacheManager(false)) {
         @Override
         public void call() {
            ConfigurationBuilder builder = new ConfigurationBuilder();
            builder.invocationBatching().disable();
            Configuration newConfig = cm.defineConfiguration("new-cache", builder.build());
            assert !newConfig.invocationBatching().enabled();

            builder = new ConfigurationBuilder();
            builder.invocationBatching().enable();
            Configuration newConfig2 = cm.defineConfiguration("new-cache", builder.build());
            assert newConfig2.invocationBatching().enabled();
            assert cm.getCache("new-cache").getCacheConfiguration().invocationBatching().enabled();
         }
      });
   }

   public void testGetCacheNames() {
      EmbeddedCacheManager cm = createCacheManager(false);
      try {
         cm.defineConfiguration("one", new ConfigurationBuilder().build());
         cm.defineConfiguration("two", new ConfigurationBuilder().build());
         cm.getCache("three");
         Set<String> cacheNames = cm.getCacheNames();
         assert 3 == cacheNames.size();
         assert cacheNames.contains("one");
         assert cacheNames.contains("two");
         assert cacheNames.contains("three");
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

   @Test(expectedExceptions = IllegalStateException.class)
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

   @Test(expectedExceptions = IllegalStateException.class)
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
         assert !store.isEmpty();
         assert 0 != data.size();
         manager.removeCache("cache");
         assert store.isEmpty();
         assert 0 == data.size();
         // Try removing the cache again, it should be a no-op
         manager.removeCache("cache");
         assert store.isEmpty();
         assert 0 == data.size();
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

   public void testRemoveCacheClusteredLocalStores(Method m) throws Exception {
      doTestRemoveCacheClustered(m, false);
   }

   public void testRemoveCacheClusteredSharedStores(Method m) throws Exception {
      doTestRemoveCacheClustered(m, true);
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
      gcb.globalJmxStatistics().allowDuplicateDomains(true);
      return TestCacheManagerFactory.createClusteredCacheManager(gcb, c);
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
            assert cache1 != null;
            assert cache2 != null;
            assert manager1.cacheExists("cache");
            assert manager2.cacheExists("cache");
            cache1.put(k(m, 1), v(m, 1));
            cache1.put(k(m, 2), v(m, 2));
            cache1.put(k(m, 3), v(m, 3));
            cache2.put(k(m, 4), v(m, 4));
            cache2.put(k(m, 5), v(m, 5));
            DummyInMemoryStore store1 = getDummyStore(cache1);
            DataContainer data1 = getDataContainer(cache1);
            DummyInMemoryStore store2 = getDummyStore(cache2);
            DataContainer data2 = getDataContainer(cache2);
            assert !store1.isEmpty();
            assert 5 == data1.size();
            assert !store2.isEmpty();
            assert 5 == data2.size();
            manager1.removeCache("cache");
            assert !manager1.cacheExists("cache");
            assert !manager2.cacheExists("cache");
            assert null == manager1.getCache("cache", false);
            assert null == manager2.getCache("cache", false);
            assert store1.isEmpty();
            assert 0 == data1.size();
            assert store2.isEmpty();
            assert 0 == data2.size();
         }
      });
   }

   private DummyInMemoryStore getDummyStore(Cache<String, String> cache1) {
      return (DummyInMemoryStore) TestingUtil.getFirstLoader(cache1);
   }

   private DataContainer getDataContainer(Cache<String, String> cache) {
      return TestingUtil.extractComponent(cache, DataContainer.class);
   }

}
