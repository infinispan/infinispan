package org.infinispan.eviction;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

/**
 * Tests size-based eviction with concurrent read and/or write operation with passivation enabled.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "eviction.EvictionWithPassivationAndConcurrentOperationsTest")
public class EvictionWithPassivationAndConcurrentOperationsTest extends EvictionWithConcurrentOperationsTest {

   @SuppressWarnings("unchecked")
   @Override
   protected void initializeKeyAndCheckData(Object key, Object value) throws CacheLoaderException {
      assertTrue("A cache store should be configured!", cache.getCacheConfiguration().loaders().usingCacheLoaders());
      cache.put(key, value);
      DataContainer container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry entry = container.get(key);
      CacheLoader loader = TestingUtil.getCacheLoader(cache);
      assertNotNull("Key " + key + " does not exist in data container.", entry);
      assertEquals("Wrong value for key " + key + " in data container.", value, entry.getValue());
      InternalCacheEntry entryLoaded = loader.load(key);
      assertNull("Key " + key + " exists in cache loader.", entryLoaded);
   }

   @SuppressWarnings("unchecked")
   @Override
   protected void assertInMemory(Object key, Object value) throws CacheLoaderException {
      DataContainer container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry entry = container.get(key);
      CacheLoader loader = TestingUtil.getCacheLoader(cache);
      assertNotNull("Key " + key + " does not exist in data container", entry);
      assertEquals("Wrong value for key " + key + " in data container", value, entry.getValue());
      InternalCacheEntry entryLoaded = loader.load(key);
      assertNull("Key " + key + " exists in cache loader", entryLoaded);
   }

   @SuppressWarnings("unchecked")
   @Override
   protected void assertNotInMemory(Object key, Object value) throws CacheLoaderException {
      DataContainer container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry entry = container.get(key);
      CacheLoader loader = TestingUtil.getCacheLoader(cache);
      assertNull("Key " + key + " exists in data container", entry);
      InternalCacheEntry entryLoaded = loader.load(key);
      assertNotNull("Key " + key + " does not exist in cache loader", entryLoaded);
      assertEquals("Wrong value for key " + key + " in cache loader", value, entryLoaded.getValue());
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder) {
      builder.loaders().passivation(true)
            .addStore(DummyInMemoryCacheStoreConfigurationBuilder.class)
            .storeName(storeName + storeNamePrefix.getAndIncrement());
   }
}
