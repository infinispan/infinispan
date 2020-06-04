package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.interceptors.impl.CacheWriterInterceptor;
import org.infinispan.interceptors.impl.DistCacheWriterInterceptor;
import org.infinispan.interceptors.impl.ScatteredCacheWriterInterceptor;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
/**
 * DistSyncCacheStoreTest.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class BaseDistStoreTest<K, V, C extends BaseDistStoreTest> extends BaseDistFunctionalTest<K, V> {
   protected boolean shared;
   protected boolean preload;
   protected boolean segmented;

   protected C shared(boolean shared) {
      this.shared = shared;
      return (C) this;
   }

   protected C preload(boolean preload) {
      this.preload = preload;
      return (C) this;
   }

   protected C segmented(boolean segmented) {
      this.segmented = segmented;
      return (C) this;
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "shared", "preload", "segmented");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), shared, preload, segmented);
   }

   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder cfg = super.buildConfiguration();
      StoreConfigurationBuilder<?, ?> storeConfigurationBuilder;
      if (shared) {
         storeConfigurationBuilder = cfg.persistence().addStore(
               new DummyInMemoryStoreConfigurationBuilder(cfg.persistence()).storeName(getClass().getSimpleName()));
      } else {
         storeConfigurationBuilder = cfg.persistence().addStore(
               new DummyInMemoryStoreConfigurationBuilder(cfg.persistence()));
      }
      storeConfigurationBuilder
               .shared(shared)
               .preload(preload)
               .segmented(segmented);
      return cfg;
   }

   protected int getCacheStoreStats(Cache<?, ?> cache, String cacheStoreMethod) {
      DummyInMemoryStore dummyInMemoryStore = TestingUtil.getFirstStore(cache);
      return dummyInMemoryStore.stats().get(cacheStoreMethod);
   }

   protected void assertNumberOfInvocations(DummyInMemoryStore dims, String method, int expected) {
      assertEquals(expected, dims.stats().get(method).intValue());
   }

   protected void clearStats(Cache<?, ?> cache) {
      DummyInMemoryStore store = TestingUtil.getFirstStore(cache);
      store.clearStats();

      CacheWriterInterceptor cacheWriterInterceptor = getCacheWriterInterceptor(cache);
      if (cacheWriterInterceptor != null) {
         cacheWriterInterceptor.resetStatistics();
      }
   }

   protected CacheWriterInterceptor getCacheWriterInterceptor(Cache<?, ?> cache) {
      CacheWriterInterceptor cacheWriterInterceptor = TestingUtil.extractComponent(cache, DistCacheWriterInterceptor.class);
      if (cacheWriterInterceptor == null) {
         cacheWriterInterceptor = TestingUtil.extractComponent(cache, ScatteredCacheWriterInterceptor.class);
      }
      return cacheWriterInterceptor;
   }
}
