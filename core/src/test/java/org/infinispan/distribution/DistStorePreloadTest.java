package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Test preloading with a distributed cache.
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @since 5.1
 */
@Test(groups = "functional", testName = "distribution.DistStorePreloadTest")
public class DistStorePreloadTest<D extends DistStorePreloadTest<D>> extends BaseDistStoreTest<String, String, D> {

   public static final int NUM_KEYS = 10;

   public DistStorePreloadTest() {
      INIT_CLUSTER_SIZE = 1;
      testRetVals = true;
      // Have to be shared and preload
      shared = true;
      preload = true;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new DistStorePreloadTest<D>().segmented(true).transactional(false),
            new DistStorePreloadTest<D>().segmented(true).transactional(true),
            new DistStorePreloadTest<D>().segmented(false).transactional(false),
            new DistStorePreloadTest<D>().segmented(false).transactional(true),
      };
   }

   @AfterMethod
   public void clearStats() {
      for (Cache<String, String> c: caches) {
         log.trace("Clearing stats for cache store on cache "+ c);
         DummyInMemoryStore<String, String> store = TestingUtil.getFirstStore(c);
         store.clear();
      }

      // Make sure to clean up any extra caches
      if (managers().length > 1) {
         killMember(1, cacheName);
      }
   }

   public void testPreloadOnStart() throws PersistenceException {
      for (int i = 0; i < NUM_KEYS; i++) {
         c1.put("k" + i, "v" + i);
      }
      DataContainer<String, String> dc1 = c1.getAdvancedCache().getDataContainer();
      assert dc1.size() == NUM_KEYS;

      DummyInMemoryStore<String, String> store = TestingUtil.getFirstStore(c1);
      assertEquals(NUM_KEYS, store.size());

      addClusterEnabledCacheManager(TestDataSCI.INSTANCE, null, new TransportFlags().withFD(false));
      EmbeddedCacheManager cm2 = cacheManagers.get(1);
      cm2.defineConfiguration(cacheName, buildConfiguration().build());
      c2 = cache(1, cacheName);
      caches.add(c2);
      waitForClusterToForm(cacheName);

      DataContainer<String, String> dc2 = c2.getAdvancedCache().getDataContainer();
      assertEquals("Expected all the cache store entries to be preloaded on the second cache", NUM_KEYS, dc2.size());

      for (int i = 0; i < NUM_KEYS; i++) {
         assertOwnershipAndNonOwnership("k" + i, true);
      }
   }

   public void testPreloadExpirationMemoryPresent() {
      testPreloadExpiration(true);
   }

   public void testPreloadExpirationNoMemoryPresent() {
      testPreloadExpiration(false);
   }

   private void testPreloadExpiration(boolean hasMemoryContents) {
      ControlledTimeService timeService = new ControlledTimeService();
      TestingUtil.replaceComponent(c1.getCacheManager(), TimeService.class, timeService, true);

      long createdTime = timeService.wallClockTime();

      String key = "key";
      String value = "value";
      c1.put(key, value, 10, TimeUnit.MINUTES);

      DataContainer<String, String> dc1 = c1.getAdvancedCache().getDataContainer();
      CacheEntry<?, ?> entry = dc1.peek(key);
      assertNotNull(entry);
      assertEquals(createdTime, entry.getCreated());

      if (!hasMemoryContents) {
         dc1.clear();
      }

      timeService.advance(1000);

      DummyInMemoryStore store = TestingUtil.getFirstStore(c1);
      assertEquals(1, store.getStoreDataSize());

      addClusterEnabledCacheManager();
      EmbeddedCacheManager cm2 = cacheManagers.get(1);
      TestingUtil.replaceComponent(cm2, TimeService.class, timeService, true);
      cm2.defineConfiguration(cacheName, buildConfiguration().build());
      c2 = cache(1, cacheName);
      caches.add(c2);
      waitForClusterToForm(cacheName);

      DataContainer<String, String> dc2 = c2.getAdvancedCache().getDataContainer();
      entry = dc2.peek(key);
      assertNotNull(entry);
      // Created time should be the same, not the incremented one
      assertEquals(createdTime, entry.getCreated());
   }
}
