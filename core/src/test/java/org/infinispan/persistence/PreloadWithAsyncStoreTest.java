package org.infinispan.persistence;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL;
import static org.infinispan.transaction.TransactionMode.TRANSACTIONAL;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "persistence.PreloadWithAsyncStoreTest")
public class PreloadWithAsyncStoreTest extends SingleCacheManagerTest {
   private static final Object[] KEYS = new Object[]{"key_1", "key_2", "key_3", "key_4"};
   private static final Object[] VALUES = new Object[]{"value_1", "value_2", "value_3", "value_4"};

   public void testtPreloadWithNonTransactionalCache() throws Exception {
      doTest(CacheType.NO_TRANSACTIONAL);
   }

   public void testtPreloadWithTransactionalUsingSynchronizationCache() throws Exception {
      doTest(CacheType.TRANSACTIONAL_SYNCHRONIZATION);
   }

   public void testPreloadWithTransactionalUsingXACache() throws Exception {
      doTest(CacheType.TRANSACTIONAL_XA);
   }

   public void testPreloadWithTransactionalUsingXAAndRecoveryCache() throws Exception {
      doTest(CacheType.TRANSACTIONAL_XA_RECOVERY);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder().clusteredDefault();
      TestCacheManagerFactory.addInterceptor(global, name -> name.contains("TX"), new ExceptionTrackerInterceptor(), TestCacheManagerFactory.InterceptorPosition.FIRST, null);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(global, new ConfigurationBuilder());

      for (CacheType cacheType : CacheType.values()) {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         builder.persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class)
               .preload(true)
               .storeName(this.getClass().getName()).async().enable();
         builder.transaction().transactionMode(cacheType.transactionMode).useSynchronization(cacheType.useSynchronization)
               .recovery().enabled(cacheType.useRecovery);
         cm.defineConfiguration(cacheType.cacheName, builder.build());
      }

      return cm;
   }

   protected void doTest(CacheType cacheType) throws Exception {
      final Cache<Object, Object> cache = cacheManager.getCache(cacheType.cacheName);
      ExceptionTrackerInterceptor interceptor = getInterceptor(cache);

      assertTrue("Preload should be enabled.", cache.getCacheConfiguration().persistence().preload());
      assertTrue("Async Store should be enabled.", cache.getCacheConfiguration().persistence().usingAsyncStore());

      WaitNonBlockingStore<Object, Object> store =  TestingUtil.getFirstStoreWait(cache);

      assertNotInCacheAndStore(cache, store, KEYS);

      for (int i = 0; i < KEYS.length; ++i) {
         cache.put(KEYS[i], VALUES[i]);
      }

      for (int i = 1; i < KEYS.length; i++) {
         assertInCacheAndStore(cache, store, KEYS[i], VALUES[i]);
      }

      DataContainer<Object, Object> dataContainer = cache.getAdvancedCache().getDataContainer();

      assertEquals("Wrong number of keys in data container after puts.", KEYS.length, dataContainer.size());
      assertEquals("Some exceptions has been caught during the puts.", 0, interceptor.exceptionsCaught.get());
      cache.stop();
      assertEquals("Expected empty data container after stop.", 0, dataContainer.size());
      assertEquals("Some exceptions has been caught during the stop.", 0, interceptor.exceptionsCaught.get());

      cache.start();
      assertTrue("Preload should be enabled after restart.", cache.getCacheConfiguration().persistence().preload());
      assertTrue("Async Store should be enabled after restart.", cache.getCacheConfiguration().persistence().usingAsyncStore());

      dataContainer = cache.getAdvancedCache().getDataContainer();
      assertEquals("Wrong number of keys in data container after preload.", KEYS.length, dataContainer.size());
      assertEquals("Some exceptions has been caught during the preload.", 0, interceptor.exceptionsCaught.get());

      // Re-retrieve since the old reference might not be usable
      store = TestingUtil.getStoreWait(cache, 0, false);
      for (int i = 1; i < KEYS.length; i++) {
         assertInCacheAndStore(cache, store, KEYS[i], VALUES[i]);
      }
   }

   private void assertInCacheAndStore(Cache<Object, Object> cache, WaitNonBlockingStore<Object, Object> loader, Object key, Object value) throws PersistenceException {
      InternalCacheValue<Object> se = cache.getAdvancedCache().getDataContainer().peek(key).toInternalCacheValue();
      assertStoredEntry(se.getValue(), value, "Cache", key);
      MarshallableEntry<Object, Object> me = loader.loadEntry(key);
      assertStoredEntry(me.getValue(), value, "Store", key);
   }

   private void assertStoredEntry(Object value, Object expectedValue, String src, Object key) {
      assertNotNull(src + " entry for key " + key + " should NOT be null", value);
      assertEquals(src + " should contain value " + expectedValue + " under key " + key + " but was " + value, expectedValue, value);
   }

   private <T> void assertNotInCacheAndStore(Cache<Object, Object> cache, WaitNonBlockingStore<Object, Object> store, T... keys) throws PersistenceException {
      for (Object key : keys) {
         assertFalse("Cache should not contain key " + key, cache.getAdvancedCache().getDataContainer().containsKey(key));
         assertFalse("Store should not contain key " + key, store.contains(key));
      }
   }

   private ExceptionTrackerInterceptor getInterceptor(Cache<Object, Object> cache) {
      return extractInterceptorChain(cache)
            .findInterceptorWithClass(ExceptionTrackerInterceptor.class);
   }

   private enum CacheType {
      NO_TRANSACTIONAL("NO_TX"),
      TRANSACTIONAL_SYNCHRONIZATION(TRANSACTIONAL, "TX_SYNC", true, false),
      TRANSACTIONAL_XA(TRANSACTIONAL, "TX_XA", false, false),
      TRANSACTIONAL_XA_RECOVERY(TRANSACTIONAL, "TX_XA_RECOVERY", false, true);
      final TransactionMode transactionMode;
      final String cacheName;
      final boolean useSynchronization;
      final boolean useRecovery;

      CacheType(TransactionMode transactionMode, String cacheName, boolean useSynchronization,
            boolean useRecovery) {
         this.transactionMode = transactionMode;
         this.cacheName = cacheName;
         this.useSynchronization = useSynchronization;
         this.useRecovery = useRecovery;
      }

      CacheType(String cacheName) {
         //no tx cache. the boolean parameters are ignored.
         this(NON_TRANSACTIONAL, cacheName, false, false);
      }
   }

   static class ExceptionTrackerInterceptor extends BaseAsyncInterceptor {

      private final AtomicInteger exceptionsCaught = new AtomicInteger();

      @Override
      public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
         return invokeNextAndExceptionally(ctx, command, (rCtx, rCommand, throwable) -> {
            exceptionsCaught.incrementAndGet();
            throw throwable;
         });
      }
   }

}
