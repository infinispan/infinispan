package org.infinispan.eviction.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Tests size-based eviction with concurrent read and/or write operation with passivation enabled.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "eviction.EvictionWithPassivationAndConcurrentOperationsTest")
public class EvictionWithPassivationAndConcurrentOperationsTest extends EvictionWithConcurrentOperationsTest {

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
      MarshallableEntry<Object, Object> entryLoaded = loader.loadEntry(key);
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
      MarshallableEntry<Object, Object> entryLoaded = loader.loadEntry(key);
      assertNull("Key " + key + " exists in cache loader", entryLoaded);
   }

   @SuppressWarnings("unchecked")
   @Override
   protected void assertNotInMemory(Object key, Object value) {
      DataContainer container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry entry = container.get(key);
      CacheLoader<Object, Object> loader = TestingUtil.getFirstLoader(cache);
      assertNull("Key " + key + " exists in data container", entry);
      MarshallableEntry<Object, Object> entryLoaded = loader.loadEntry(key);
      assertNotNull("Key " + key + " does not exist in cache loader", entryLoaded);
      assertEquals("Wrong value for key " + key + " in cache loader", value, entryLoaded.getValue());
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder) {
      builder.persistence().passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(storeName + storeNamePrefix.getAndIncrement());
   }
}
