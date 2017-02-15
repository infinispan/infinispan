package org.infinispan.statetransfer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

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

   static final int INSERTION_COUNT = 500;

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
   protected EmbeddedCacheManager createCacheManager(String cacheName) {
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
      cm.defineConfiguration(cacheName, configurationBuilder.build());
      return cm;
   }

   public void testSharedFetchedStoreEntriesUnaffected() throws Exception {
      Cache<Object, Object> cache1, cache2, cache3;
      EmbeddedCacheManager cm1 = createCacheManager(cacheName);
      cache1 = cm1.getCache(cacheName);
      writeLargeInitialData(cache1);

      assertEquals(INSERTION_COUNT, getDataContainerSize(cache1));

      verifyInitialDataOnLoader(cache1);

      EmbeddedCacheManager cm2 = createCacheManager(cacheName);
      cache2 = cm2.getCache(cacheName);
      TestingUtil.waitForStableTopology(cache1, cache2);

      assertEquals(INSERTION_COUNT, getDataContainerSize(cache1));
      assertEquals(INSERTION_COUNT, getDataContainerSize(cache2));

      verifyInitialDataOnLoader(cache2);

      EmbeddedCacheManager cm3 = createCacheManager(cacheName);
      cache3 = cm3.getCache(cacheName);
      TestingUtil.waitForStableTopology(cache1, cache2, cache3);

      // Need an additional wait for the non-owned entries to be deleted from the data containers
      eventuallyEquals(INSERTION_COUNT * 2, () -> getDataContainerSize(cache1, cache2, cache3));

      // Shared cache loader should have all the contents still
      verifyInitialDataOnLoader(cache3);
   }

   public void testUnsharedNotFetchedStoreEntriesRemovedProperly() throws Exception {
      sharedCacheLoader.set(false);
      fetchPersistentState.set(false);

      Cache<Object, Object> cache1, cache2, cache3;
      EmbeddedCacheManager cm1 = createCacheManager(cacheName);
      cache1 = cm1.getCache(cacheName);
      writeLargeInitialData(cache1);

      assertEquals(INSERTION_COUNT, cache1.getAdvancedCache().getDataContainer().size());

      verifyInitialDataOnLoader(cache1);

      EmbeddedCacheManager cm2 = createCacheManager(cacheName);
      cache2 = cm2.getCache(cacheName);
      TestingUtil.waitForStableTopology(cache1, cache2);

      assertEquals(INSERTION_COUNT, getDataContainerSize(cache1));
      assertEquals(INSERTION_COUNT, getDataContainerSize(cache2));

      verifyCacheLoaderCount(INSERTION_COUNT, cache2);

      EmbeddedCacheManager cm3 = createCacheManager(cacheName);
      cache3 = cm3.getCache(cacheName);
      TestingUtil.waitForStableTopology(cache1, cache2, cache3);

      // Need an additional wait for the non-owned entries to be deleted from the data containers
      eventuallyEquals(INSERTION_COUNT * 2, () -> getDataContainerSize(cache1, cache2, cache3));

      // TODO Shouldn't this work?
      //verifyCacheLoaderCount(INSERTION_COUNT * 2, cache1, cache2, cache3);
   }

   public void testUnsharedFetchedStoreEntriesRemovedProperly() throws Exception {
      sharedCacheLoader.set(false);

      Cache<Object, Object> cache1, cache2, cache3;
      EmbeddedCacheManager cm1 = createCacheManager(cacheName);
      cache1 = cm1.getCache(cacheName);
      writeLargeInitialData(cache1);

      assertEquals(INSERTION_COUNT, cache1.getAdvancedCache().getDataContainer().size());

      verifyInitialDataOnLoader(cache1);

      EmbeddedCacheManager cm2 = createCacheManager(cacheName);
      cache2 = cm2.getCache(cacheName);
      TestingUtil.waitForStableTopology(cache1, cache2);

      assertEquals(INSERTION_COUNT, cache1.getAdvancedCache().getDataContainer().size());
      assertEquals(INSERTION_COUNT, cache2.getAdvancedCache().getDataContainer().size());

      verifyCacheLoaderCount(INSERTION_COUNT, cache2);

      EmbeddedCacheManager cm3 = createCacheManager(cacheName);
      cache3 = cm3.getCache(cacheName);
      TestingUtil.waitForStableTopology(cache1, cache2, cache3);

      // Need an additional wait for the non-owned entries to be deleted from the data containers
      eventuallyEquals(INSERTION_COUNT * 2, () -> getDataContainerSize(cache1, cache2, cache3));

      // TODO Shouldn't this work?
      //verifyCacheLoaderCount(INSERTION_COUNT * 2, cache1, cache2, cache3);
   }

   public void testSharedNotFetchedStoreEntriesRemovedProperly() throws Exception {
      fetchPersistentState.set(false);

      Cache<Object, Object> cache1, cache2, cache3;
      EmbeddedCacheManager cm1 = createCacheManager(cacheName);
      cache1 = cm1.getCache(cacheName);
      writeLargeInitialData(cache1);

      assertEquals(INSERTION_COUNT, cache1.getAdvancedCache().getDataContainer().size());

      verifyInitialDataOnLoader(cache1);

      EmbeddedCacheManager cm2 = createCacheManager(cacheName);
      cache2 = cm2.getCache(cacheName);
      TestingUtil.waitForStableTopology(cache1, cache2);

      assertEquals(INSERTION_COUNT, cache1.getAdvancedCache().getDataContainer().size());
      assertEquals(INSERTION_COUNT, cache2.getAdvancedCache().getDataContainer().size());

      verifyCacheLoaderCount(INSERTION_COUNT, cache2);

      EmbeddedCacheManager cm3 = createCacheManager(cacheName);
      cache3 = cm3.getCache(cacheName);
      TestingUtil.waitForStableTopology(cache1, cache2, cache3);
      // Shared cache loader should have all the contents still
      verifyInitialDataOnLoader(cache3);

      // Need an additional wait for the non-owned entries to be deleted from the data containers
      eventuallyEquals(INSERTION_COUNT * 2, () -> getDataContainerSize(cache1, cache2, cache3));
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

   private void verifyCacheLoaderCount(int expectedCount, Cache... caches) {
      int count = 0;
      for (Cache<Object, Object> cache : caches) {
         count += ((AdvancedCacheLoader) TestingUtil.getFirstLoader(cache)).size();
      }
      assertEquals(expectedCount, count);
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
