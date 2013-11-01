package org.infinispan.jcache;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.jcache.util.InMemoryJCacheLoader;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CompletionListenerFuture;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.infinispan.jcache.util.JCacheTestingUtil.*;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Tests JCache behaivour when plugged with cache loaders.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups = "functional", testName = "jcache.JCacheLoaderTest")
public class JCacheLoaderTest {

   public void testLoadAllWithJCacheLoader(Method m) {
      final String cacheName = m.getName();
//      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
//      globalBuilder.asyncTransportExecutor().addProperty("maxThreads", "1");
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(false)) {
         @Override
         public void call() {
            JCacheManager jCacheManager = createJCacheManager(cm, this);

            InMemoryJCacheLoader<Integer, String> cacheLoader = new InMemoryJCacheLoader<Integer, String>();
            cacheLoader.store(1, "v1").store(2, "v2");

            MutableConfiguration<Integer, String> cfg = new MutableConfiguration<Integer, String>();
            // JDK6 fails to compile when calling FactoryBuilder.factoryOf() :(
            cfg.setCacheLoaderFactory(new FactoryBuilder.SingletonFactory(cacheLoader));
            Cache<Integer, String> cache = jCacheManager.createCache(cacheName, cfg);

            assertEquals(0, cacheLoader.getLoadCount());

            CompletionListenerFuture future = new CompletionListenerFuture();
            cache.loadAll(CollectionFactory.makeSet(1, 2), true, future);

            futureGet(future);

            assertEquals(2, cacheLoader.getLoadCount());
         }
      });
   }

   public void testLoadAllWithInfinispanCacheLoader() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(false)) {
         @Override
         public void call() {
            ConfigurationBuilder builder = new ConfigurationBuilder();
            builder.persistence()
                  .addStore(DummyInMemoryStoreConfigurationBuilder.class)
                  .storeName(this.getClass().getName());

            cm.defineConfiguration("dummyStore", builder.build());
            JCacheManager jCacheManager = createJCacheManager(cm, this);
            Cache<Integer, String> cache = jCacheManager.getCache("dummyStore");

            // Load initial data in cache store
            int numEntries = loadInitialData(cm);
            DummyInMemoryStore dummyStore = TestingUtil.getFirstWriter(cm.getCache("dummyStore"));

            // Load all from cache store
            CompletionListenerFuture future = new CompletionListenerFuture();
            Set<Integer> keys = Collections.singleton(1);
            cache.loadAll(keys, false, future);
            futureGet(future); // wait for key to be loaded
            assertTrue(future.isDone());
            assertEquals(numEntries, dummyStore.stats().get("load").intValue());

            // Load from memory
            assertEquals("v1", cache.get(1));

            // Load again from cache store, overriding in-memory contents
            future = new CompletionListenerFuture();
            cache.loadAll(keys, true, future);
            futureGet(future); // wait for key to be loaded
            assertTrue(future.isDone());
            assertEquals(numEntries * 2, dummyStore.stats().get("load").intValue());
         }
      });
   }

   private Void futureGet(CompletionListenerFuture future) {
      try {
         return future.get();
      } catch (Throwable t) {
         throw new AssertionError(t);
      }
   }

   private static int loadInitialData(EmbeddedCacheManager cm) {
      TestingUtil.writeToAllStores(1, "v1", cm.getCache("dummyStore"));
      return 1;
   }

}
