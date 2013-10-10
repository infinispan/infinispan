package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Added tests that verify when a rehash occurs that the store contents are updated properly
 *
 * @author William Burns
 * @since 6.0
 */
@Test(groups = "functional", testName = "statetransfer.StateTransferDistSharedCacheLoaderFunctionalTest")
public class StateTransferDistSharedCacheLoaderFunctionalTest extends StateTransferFunctionalTest {

   ThreadLocal<Boolean> sharedCacheLoader = new ThreadLocal<Boolean>() {
      @Override
      protected Boolean initialValue() {
         return true;
      }
   };
   ThreadLocal<Boolean> fetchPersistentState = new ThreadLocal<Boolean>() {
      @Override
      protected Boolean initialValue() {
         return true;
      }
   };
   int id;

   static final int INSERTION_COUNT = 1000;

   @BeforeMethod
   public void beforeEachMethod() {
      sharedCacheLoader.set(true);
      fetchPersistentState.set(true);
   }


   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      configurationBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      configurationBuilder.persistence().clearStores();
      DummyInMemoryStoreConfigurationBuilder dimcs = new DummyInMemoryStoreConfigurationBuilder(configurationBuilder.persistence());
      if (sharedCacheLoader.get()) {
         dimcs.storeName(getClass().getName());
      } else {
         dimcs.storeName(getClass().getName() + id++);
      }
      dimcs.fetchPersistentState(false).purgeOnStartup(false).shared(sharedCacheLoader.get()).preload(true);
      configurationBuilder.persistence().passivation(false).addStore(dimcs).fetchPersistentState(fetchPersistentState.get());
      // Want to enable eviction, but don't actually evict anything
      configurationBuilder.eviction().strategy(EvictionStrategy.LIRS).maxEntries(INSERTION_COUNT * 10);

      EmbeddedCacheManager cm = addClusterEnabledCacheManager(configurationBuilder, new TransportFlags().withMerge(true));
      return cm;
   }

   public void testSharedFetchedStoreEntriesUnaffected() throws Exception {
      Cache<Object, Object> cache1, cache2, cache3;
      EmbeddedCacheManager cm1 = createCacheManager();
      cache1 = cm1.getCache(cacheName);
      writeLargeInitialData(cache1);

      assertEquals(INSERTION_COUNT, getDataContainerSize(cache1));

      verifyInitialDataOnLoader(cache1);

      JoiningNode node = new JoiningNode(createCacheManager());
      cache2 = node.getCache(cacheName);
      node.waitForJoin(60000, cache1, cache2);

      assertEquals(INSERTION_COUNT, getDataContainerSize(cache1));
      assertEquals(INSERTION_COUNT, getDataContainerSize(cache2));

      node.verifyStateTransfer(new CacheLoaderVerifier(cache2));

      JoiningNode node2 = new JoiningNode(createCacheManager());
      cache3 = node2.getCache(cacheName);
      node2.waitForJoin(60000, cache1, cache2, cache3);
      // Shared cache loader should have all the contents still
      node2.verifyStateTransfer(new CacheLoaderVerifier(cache3));

      assertEquals(INSERTION_COUNT * 2, getDataContainerSize(cache1, cache2, cache3));
   }

   public void testUnsharedNotFetchedStoreEntriesRemovedProperly() throws Exception {
      sharedCacheLoader.set(false);
      fetchPersistentState.set(false);

      Cache<Object, Object> cache1, cache2, cache3;
      EmbeddedCacheManager cm1 = createCacheManager();
      cache1 = cm1.getCache(cacheName);
      writeLargeInitialData(cache1);

      assertEquals(INSERTION_COUNT, cache1.getAdvancedCache().getDataContainer().size());

      verifyInitialDataOnLoader(cache1);

      JoiningNode node = new JoiningNode(createCacheManager());
      cache2 = node.getCache(cacheName);
      node.waitForJoin(60000, cache1, cache2);

      assertEquals(INSERTION_COUNT, getDataContainerSize(cache1));
      assertEquals(INSERTION_COUNT, getDataContainerSize(cache2));

      node.verifyStateTransfer(new CacheLoaderCounter(INSERTION_COUNT, cache2));

      JoiningNode node2 = new JoiningNode(createCacheManager());
      cache3 = node2.getCache(cacheName);
      node2.waitForJoin(60000, cache1, cache2, cache3);
      node2.verifyStateTransfer(new CacheLoaderCounter(INSERTION_COUNT * 2, cache1, cache2, cache3));

      assertEquals(INSERTION_COUNT * 2, getDataContainerSize(cache1, cache2, cache3));
   }

   public void testUnsharedFetchedStoreEntriesRemovedProperly() throws Exception {
      sharedCacheLoader.set(false);

      Cache<Object, Object> cache1, cache2, cache3;
      EmbeddedCacheManager cm1 = createCacheManager();
      cache1 = cm1.getCache(cacheName);
      writeLargeInitialData(cache1);

      assertEquals(INSERTION_COUNT, cache1.getAdvancedCache().getDataContainer().size());

      verifyInitialDataOnLoader(cache1);

      JoiningNode node = new JoiningNode(createCacheManager());
      cache2 = node.getCache(cacheName);
      node.waitForJoin(60000, cache1, cache2);

      assertEquals(INSERTION_COUNT, cache1.getAdvancedCache().getDataContainer().size());
      assertEquals(INSERTION_COUNT, cache2.getAdvancedCache().getDataContainer().size());

      node.verifyStateTransfer(new CacheLoaderCounter(INSERTION_COUNT, cache2));

      JoiningNode node2 = new JoiningNode(createCacheManager());
      cache3 = node2.getCache(cacheName);
      node2.waitForJoin(60000, cache1, cache2, cache3);
      node2.verifyStateTransfer(new CacheLoaderCounter(INSERTION_COUNT * 2, cache1, cache2, cache3));

      assertEquals(INSERTION_COUNT * 2, getDataContainerSize(cache1, cache2, cache3));
   }

   public void testSharedNotFetchedStoreEntriesRemovedProperly() throws Exception {
      fetchPersistentState.set(false);

      Cache<Object, Object> cache1, cache2, cache3;
      EmbeddedCacheManager cm1 = createCacheManager();
      cache1 = cm1.getCache(cacheName);
      writeLargeInitialData(cache1);

      assertEquals(INSERTION_COUNT, cache1.getAdvancedCache().getDataContainer().size());

      verifyInitialDataOnLoader(cache1);

      JoiningNode node = new JoiningNode(createCacheManager());
      cache2 = node.getCache(cacheName);
      node.waitForJoin(60000, cache1, cache2);

      assertEquals(INSERTION_COUNT, cache1.getAdvancedCache().getDataContainer().size());
      assertEquals(INSERTION_COUNT, cache2.getAdvancedCache().getDataContainer().size());

      node.verifyStateTransfer(new CacheLoaderCounter(INSERTION_COUNT, cache2));

      JoiningNode node2 = new JoiningNode(createCacheManager());
      cache3 = node2.getCache(cacheName);
      node2.waitForJoin(60000, cache1, cache2, cache3);
      // Shared cache loader should have all the contents still
      node2.verifyStateTransfer(new CacheLoaderVerifier(cache3));

      assertEquals(INSERTION_COUNT * 2, getDataContainerSize(cache1, cache2, cache3));
   }

   protected int getDataContainerSize(Cache<?, ?>... caches) {
      int count = 0;
      for (Cache<?, ?> c : caches) {
         count += c.getAdvancedCache().getDataContainer().size();
      }
      return count;
   }

   protected void writeLargeInitialData(final Cache<Object, Object> c) {
      for (int i = 0; i < INSERTION_COUNT; ++i) {
         c.put("key " + i, "value " + i);
      }
   }

   public class CacheLoaderVerifier implements Callable<Void> {

      private final Cache<Object, Object> cache;

      public CacheLoaderVerifier(Cache<Object, Object> cache) {
         this.cache = cache;
      }

      @Override
      public Void call() {
         verifyInitialDataOnLoader(cache);
         return null;
      }
   }

   public class CacheLoaderCounter implements Callable<Void> {

      private final Cache<Object, Object>[] caches;
      private final int expectedCount;

      public CacheLoaderCounter(int expectedCount, Cache<Object, Object>... caches) {
         this.caches = caches;
         this.expectedCount = expectedCount;
      }

      @Override
      public Void call() {
         int count = 0;
         for (Cache<Object, Object> cache : caches) {
            count += ((AdvancedCacheLoader)TestingUtil.getFirstLoader(cache)).size();
         }
         assertEquals(expectedCount, count);
         return null;
      }
   }

   protected void verifyInitialDataOnLoader(Cache<Object, Object> c) {
      CacheLoader l = TestingUtil.getFirstLoader(c);
      for (int i = 0; i < INSERTION_COUNT; ++i) {
         assertTrue("Didn't contain key " + i, l.contains("key " + i));
      }

      for (int i = 0; i < INSERTION_COUNT; ++i) {
         assertEquals("value " + i, l.load("key " + i).getValue());
      }
   }
}
