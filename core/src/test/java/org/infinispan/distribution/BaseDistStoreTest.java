package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.support.ComposedSegmentedLoadWriteStore;
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
      int actual;
      AdvancedLoadWriteStore cs = TestingUtil.getFirstWriter(cache);
      if (cs instanceof ComposedSegmentedLoadWriteStore) {
         AtomicInteger count = new AtomicInteger();
         ((ComposedSegmentedLoadWriteStore) cs).forEach((store, segment) ->
               count.addAndGet(((DummyInMemoryStore) store).stats().get(cacheStoreMethod)));
         actual = count.get();
      } else {
         actual = ((DummyInMemoryStore) cs).stats().get(cacheStoreMethod);
      }
      return actual;
   }

   protected void assertNumberOfInvocations(CacheLoader cs, String method, int expected) {
      int actual;
      if (cs instanceof ComposedSegmentedLoadWriteStore) {
         AtomicInteger count = new AtomicInteger();
         ((ComposedSegmentedLoadWriteStore) cs).forEach((store, segment) ->
               count.addAndGet(((DummyInMemoryStore) store).stats().get(method)));
         actual = count.get();
      } else {
         actual = ((DummyInMemoryStore) cs).stats().get(method);
      }

      assertEquals(expected, actual);
   }

   protected void clearStats(Cache<?, ?> cache) {
      AdvancedLoadWriteStore cs = TestingUtil.getFirstLoader(cache);
      if (cs instanceof ComposedSegmentedLoadWriteStore) {
         ((ComposedSegmentedLoadWriteStore) cs).forEach((store, segment) -> {
            ((DummyInMemoryStore) store).clearStats();
         });
      } else {
         ((DummyInMemoryStore) cs).clearStats();
      }
   }
}
