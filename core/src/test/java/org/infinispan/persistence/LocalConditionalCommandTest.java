package org.infinispan.persistence;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.interceptors.CacheLoaderInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Tests if the conditional commands correctly fetch the value from cache loader even with the skip cache load/store
 * flags.
 * <p/>
 * The configuration used is a non-tx non-clustered cache without passivation.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "persistence.LocalConditionalCommandTest")
public class LocalConditionalCommandTest extends SingleCacheManagerTest {

   private static final String PRIVATE_STORE_CACHE_NAME = "private-store-cache";
   private static final String SHARED_STORE_CACHE_NAME = "shared-store-cache";
   private final String key = getClass().getSimpleName() + "-key";
   private final String value1 = getClass().getSimpleName() + "-value1";
   private final String value2 = getClass().getSimpleName() + "-value2";
   private final boolean transactional;
   private final boolean passivation;

   public LocalConditionalCommandTest() {
      this(false, false);
   }

   protected LocalConditionalCommandTest(boolean transactional, boolean passivation) {
      this.transactional = transactional;
      this.passivation = passivation;
   }

   private static ConfigurationBuilder createConfiguration(String storeName, boolean shared, boolean transactional, boolean passivation) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, transactional);
      builder.jmxStatistics().enable();
      builder.persistence()
            .passivation(passivation)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(storeName + (shared ? "-shared" : "-private"))
            .fetchPersistentState(false)
            .purgeOnStartup(true)
            .shared(shared);
      return builder;
   }

   private static <K, V> void writeToStore(Cache<K, V> cache, K key, V value) {
      TestingUtil.getFirstWriter(cache).write(marshalledEntry(key, value, cache.getAdvancedCache().getComponentRegistry().getCacheMarshaller()));
   }

   private static <K, V> MarshalledEntry<K, V> marshalledEntry(K key, V value, StreamingMarshaller marshaller) {
      return new MarshalledEntryImpl<>(key, value, null, marshaller);
   }

   private static CacheLoaderInterceptor cacheLoaderInterceptor(Cache<?, ?> cache) {
      InterceptorChain chain = TestingUtil.extractComponent(cache, InterceptorChain.class);
      return (CacheLoaderInterceptor) chain.getInterceptorsWhichExtend(CacheLoaderInterceptor.class).get(0);
   }

   private void doTest(Cache<String, String> cache, ConditionalOperation operation, Flag flag) {
      assertEmpty(cache);
      initStore(cache);
      final boolean skipLoad = flag == Flag.SKIP_CACHE_LOAD || flag == Flag.SKIP_CACHE_STORE;

      try {
         if (flag != null) {
            operation.execute(cache.getAdvancedCache().withFlags(flag), key, value1, value2);
         } else {
            operation.execute(cache, key, value1, value2);
         }
      } catch (Exception e) {
         //some operation are allowed to fail. e.g. putIfAbsent.
         //we only check the final value
         log.debug(e);
      }

      assertLoadAfterOperation(cache, skipLoad);

      assertEquals(operation.finalValue(value1, value2, skipLoad), cache.get(key));
   }

   private void assertLoadAfterOperation(Cache<?, ?> cache, boolean skipLoad) {
      assertEquals("cache load", skipLoad ? 0 : 1, cacheLoaderInterceptor(cache).getCacheLoaderLoads());
   }

   private void assertEmpty(Cache<?, ?> cache) {
      assertTrue(cache + ".isEmpty()", cache.isEmpty());
   }

   private void initStore(Cache<String, String> cache) {
      writeToStore(cache, key, value1);
      assertTrue(TestingUtil.getFirstLoader(cache).contains(key));
      cacheLoaderInterceptor(cache).resetStatistics();
   }

   public void testPutIfAbsentWithSkipCacheLoader() {
      doTest(this.<String, String>cache(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Flag.SKIP_CACHE_LOAD);
   }

   public void testPutIfAbsentWithIgnoreReturnValues() {
      doTest(this.<String, String>cache(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Flag.IGNORE_RETURN_VALUES);
   }

   public void testPutIfAbsent() {
      doTest(this.<String, String>cache(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, null);
   }

   public void testReplaceWithSkipCacheLoader() {
      doTest(this.<String, String>cache(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Flag.SKIP_CACHE_LOAD);
   }

   public void testReplaceWithIgnoreReturnValues() {
      doTest(this.<String, String>cache(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Flag.IGNORE_RETURN_VALUES);
   }

   public void testReplace() {
      doTest(this.<String, String>cache(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE, null);
   }

   public void testReplaceIfWithSkipCacheLoader() {
      doTest(this.<String, String>cache(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Flag.SKIP_CACHE_LOAD);
   }

   public void testReplaceIfWithIgnoreReturnValues() {
      doTest(this.<String, String>cache(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Flag.IGNORE_RETURN_VALUES);
   }

   public void testReplaceIf() {
      doTest(this.<String, String>cache(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, null);
   }

   public void testRemoveIfWithSkipCacheLoader() {
      doTest(this.<String, String>cache(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Flag.SKIP_CACHE_LOAD);
   }

   public void testRemoveIfWithIgnoreReturnValues() {
      doTest(this.<String, String>cache(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Flag.IGNORE_RETURN_VALUES);
   }

   public void testRemoveIf() {
      doTest(this.<String, String>cache(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, null);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager embeddedCacheManager = TestCacheManagerFactory.createCacheManager();
      embeddedCacheManager.defineConfiguration(PRIVATE_STORE_CACHE_NAME, createConfiguration(getClass().getSimpleName(), false, transactional, passivation).build());
      embeddedCacheManager.defineConfiguration(SHARED_STORE_CACHE_NAME, createConfiguration(getClass().getSimpleName(), true, transactional, passivation).build());
      return embeddedCacheManager;
   }

   private static enum ConditionalOperation {
      PUT_IF_ABSENT {
         @Override
         public <K, V> void execute(Cache<K, V> cache, K key, V value1, V value2) {
            cache.putIfAbsent(key, value2);
         }

         @Override
         public <V> V finalValue(V value1, V value2, boolean skipLoad) {
            return skipLoad ? value2 : value1;
         }
      },
      REPLACE {
         @Override
         public <K, V> void execute(Cache<K, V> cache, K key, V value1, V value2) {
            cache.replace(key, value2);
         }

         @Override
         public <V> V finalValue(V value1, V value2, boolean skipLoad) {
            return skipLoad ? value1 : value2;
         }
      },
      REPLACE_IF {
         @Override
         public <K, V> void execute(Cache<K, V> cache, K key, V value1, V value2) {
            cache.replace(key, value1, value2);
         }

         @Override
         public <V> V finalValue(V value1, V value2, boolean skipLoad) {
            return skipLoad ? value1 : value2;
         }
      },
      REMOVE_IF {
         @Override
         public <K, V> void execute(Cache<K, V> cache, K key, V value1, V value2) {
            cache.remove(key, value1);
         }

         @Override
         public <V> V finalValue(V value1, V value2, boolean skipLoad) {
            return skipLoad ? value1 : null;
         }
      };

      public abstract <K, V> void execute(Cache<K, V> cache, K key, V value1, V value2);

      public abstract <V> V finalValue(V value1, V value2, boolean skipLoad);
   }
}
