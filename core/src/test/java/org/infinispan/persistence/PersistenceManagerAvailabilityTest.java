package org.infinispan.persistence;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.PersistenceAvailabilityChanged;
import org.infinispan.notifications.cachelistener.event.PersistenceAvailabilityChangedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.spi.StoreUnavailableException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(testName = "persistence.PersistenceManagerAvailabilityTest", groups = "functional")
public class PersistenceManagerAvailabilityTest extends AbstractInfinispanTest {

   private Cache<Object, Object> createManagerAndGetCache(int startFailures) {
      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().globalJmxStatistics().allowDuplicateDomains(true).build();
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      Configuration config = cb.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .startFailures(startFailures)
            .build();
      return new DefaultCacheManager(globalConfiguration, config).getCache();
   }

   @Test(expectedExceptions = StoreUnavailableException.class)
   public void testCacheAvailability() {
      Cache<Object, Object> cache = createManagerAndGetCache(0);
      cache.put(1, 1);
      DummyInMemoryStore dims = TestingUtil.getFirstWriter(cache);
      dims.setAvailable(false);
      PersistenceManager pm = TestingUtil.extractComponent(cache, PersistenceManager.class);
      eventually(() -> !pm.isAvailable());
      try {
         cache.put(1, 2);
      } catch (Exception e) {
         assertEquals(1, cache.get(1));
         throw e;
      }
      TestingUtil.killCaches();
   }

   @Test(expectedExceptions = CacheException.class)
   public void testUnavailableStoreOnStart() {
      createManagerAndGetCache(11);
   }

   public void testStoreReconnect() {
      PersistenceAvailabilityListener pal = new PersistenceAvailabilityListener();
      Cache<Object, Object> cache = createManagerAndGetCache(0);
      cache.addListener(pal);
      assertEquals(0, pal.availableCount.get());
      assertEquals(0, pal.unavailableCount.get());

      cache.put(1, 1);
      PersistenceManager pm = TestingUtil.extractComponent(cache, PersistenceManager.class);
      assertTrue(pm.isAvailable());
      DummyInMemoryStore dims = TestingUtil.getFirstWriter(cache);
      dims.setAvailable(false);
      eventually(() -> !pm.isAvailable());
      eventuallyEquals(1, () -> pal.unavailableCount.get());

      try {
         cache.put(1, 2);
         fail("Expected " + StoreUnavailableException.class.getSimpleName());
      } catch (PersistenceException ignore) {
      }
      dims.setAvailable(true);
      eventually(pm::isAvailable);
      assertEquals(1, pal.availableCount.get());

      cache.put(1, 3);
      assertEquals(3, cache.get(1));
   }

   @Listener
   public static class PersistenceAvailabilityListener {
      AtomicInteger availableCount = new AtomicInteger();
      AtomicInteger unavailableCount = new AtomicInteger();

      @PersistenceAvailabilityChanged
      public void availabilityChange(PersistenceAvailabilityChangedEvent event) {
         if (event.isAvailable())
            availableCount.incrementAndGet();
         else
            unavailableCount.incrementAndGet();
      }
   }
}
