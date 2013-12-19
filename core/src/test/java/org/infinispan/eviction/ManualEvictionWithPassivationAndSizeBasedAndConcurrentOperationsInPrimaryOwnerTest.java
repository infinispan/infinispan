package org.infinispan.eviction;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.AssertJUnit.*;

/**
 * Tests manual eviction with concurrent read and/or write operation. This test has passivation enabled and the eviction
 * happens in the primary owner
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "eviction.ManualEvictionWithPassivationAndSizeBasedAndConcurrentOperationsInPrimaryOwnerTest")
public class ManualEvictionWithPassivationAndSizeBasedAndConcurrentOperationsInPrimaryOwnerTest
      extends ManualEvictionWithSizeBasedAndConcurrentOperationsInPrimaryOwnerTest {

   @Override
   public void testScenario3() throws Exception {
      final Object key1 = createSameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final Latch latch = new Latch();
      final SyncEvictionListener evictionListener = new SyncEvictionListener() {
         @CacheEntriesEvicted
         @Override
         public void evicted(CacheEntriesEvictedEvent event) {
            if (event.getEntries().containsKey(key1)) {
               latch.blockIfNeeded();
            }
         }
      };
      cache.addListener(evictionListener);

      //this will trigger the eviction of key1. key1 eviction will be blocked in the latch
      latch.enable();
      Future<Void> evict = evictWithFuture(key1);
      latch.waitToBlock(30, TimeUnit.SECONDS);

      //the eviction was trigger and the key is no longer in the map
      Future<Object> get = cache.getAsync(key1);

      try {
         get.get(10, TimeUnit.SECONDS);
         //the eviction acquires the lock to atomically passivate and remove from DC. the get acquires the lock to
         //atomically activate and put it in DC. so, we have a deadlock.
         fail("Get should not succeed while the eviction is in progress");
      } catch (TimeoutException expected) {
         //expected
      }

      //let the eviction continue and wait for put
      latch.disable();
      evict.get();
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", get.get(30, TimeUnit.SECONDS));

      assertInMemory(key1, "v1");
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder) {
      builder.persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(storeName + storeNamePrefix.getAndIncrement());
   }

   @SuppressWarnings("unchecked")
   @Override
   protected void initializeKeyAndCheckData(Object key, Object value) {
      assertTrue("A cache store should be configured!", cache.getCacheConfiguration().persistence().usingStores());
      cache.put(key, value);
      DataContainer container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry entry = container.get(key);
      CacheLoader<Object, Object> loader = TestingUtil.getFirstLoader(cache);
      assertNotNull("Key " + key + " does not exist in data container.", entry);
      assertEquals("Wrong value for key " + key + " in data container.", value, entry.getValue());
      MarshalledEntry<Object, Object> entryLoaded = loader.load(key);
      assertNull("Key " + key + " exists in cache loader.", entryLoaded);
   }

   @SuppressWarnings("unchecked")
   @Override
   protected void assertInMemory(Object key, Object value) {
      DataContainer container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry entry = container.get(key);
      CacheLoader<Object, Object> loader = TestingUtil.getFirstLoader(cache);
      assertNotNull("Key " + key + " does not exist in data container", entry);
      assertEquals("Wrong value for key " + key + " in data container", value, entry.getValue());
      MarshalledEntry<Object, Object> entryLoaded = loader.load(key);
      assertNull("Key " + key + " exists in cache loader.", entryLoaded);
   }

   @SuppressWarnings("unchecked")
   @Override
   protected void assertNotInMemory(Object key, Object value) {
      DataContainer container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry entry = container.get(key);
      CacheLoader<Object, Object> loader = TestingUtil.getFirstLoader(cache);
      assertNull("Key " + key + " exists in data container", entry);
      MarshalledEntry<Object, Object> entryLoaded = loader.load(key);
      assertNotNull("Key " + key + " does not exist in cache loader", entryLoaded);
      assertEquals("Wrong value for key " + key + " in cache loader", value, entryLoaded.getValue());
   }
}
