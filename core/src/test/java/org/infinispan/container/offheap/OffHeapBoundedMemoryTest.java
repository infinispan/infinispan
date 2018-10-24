package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.MemoryUnit;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.container.impl.InternalDataContainerAdapter;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.eviction.EvictionType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "container.offheap.OffHeapBoundedMemoryTest")
public class OffHeapBoundedMemoryTest extends AbstractInfinispanTest {
   public void testTooSmallToInsert() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory()
            // Only allocate enough for address count - oops
            .size(16 + MemoryConfiguration.ADDRESS_COUNT.getDefaultValue() * 8)
            .evictionType(EvictionType.MEMORY)
            .storageType(StorageType.OFF_HEAP);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(builder);
      Cache<Object, Object> smallHeapCache = manager.getCache();
      assertEquals(0, smallHeapCache.size());
      // Put something larger than size
      assertNull(smallHeapCache.put(1, 3));
      assertEquals(0, smallHeapCache.size());
   }

   private static DataContainer getContainer(AdvancedCache cache) {
      DataContainer container = cache.getDataContainer();
      if (container instanceof InternalDataContainerAdapter) {
         return ((InternalDataContainerAdapter) container).delegate();
      }
      return container;
   }

   /**
    * Test to ensure that off heap allocated amount equals the total size used by the container
    */
   public void testAllocatedAmountEqual() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory()
            .size(MemoryUnit.MEGABYTES.toBytes(20))
            .evictionType(EvictionType.MEMORY)
            .storageType(StorageType.OFF_HEAP);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(builder);
      AdvancedCache<Object, Object> cache = manager.getCache().getAdvancedCache();

      OffHeapMemoryAllocator allocator = cache.getComponentRegistry().getComponent(
            OffHeapMemoryAllocator.class);
      BoundedOffHeapDataContainer container = (BoundedOffHeapDataContainer) getContainer(cache);
      assertEquals(allocator.getAllocatedAmount(), container.currentSize);

      cache.put(1, 2);

      assertEquals(allocator.getAllocatedAmount(), container.currentSize);

      cache.clear();

      assertEquals(allocator.getAllocatedAmount(), container.currentSize);
   }

   public void testAllocatedAmountEqualWithVersion() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory()
            .size(MemoryUnit.MEGABYTES.toBytes(20))
            .evictionType(EvictionType.MEMORY)
            .storageType(StorageType.OFF_HEAP);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(builder);
      AdvancedCache<Object, Object> cache = manager.getCache().getAdvancedCache();

      cache.put(1, 2, new EmbeddedMetadata.Builder().version(new NumericVersion(23)).build());

      OffHeapMemoryAllocator allocator = cache.getComponentRegistry().getComponent(
            OffHeapMemoryAllocator.class);
      BoundedOffHeapDataContainer container = (BoundedOffHeapDataContainer) getContainer(cache);
      assertEquals(allocator.getAllocatedAmount(), container.currentSize);

      cache.clear();

      assertEquals(allocator.getAllocatedAmount(), container.currentSize);
   }

   public void testAllocatedAmountEqualWithExpiration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory()
            .size(MemoryUnit.MEGABYTES.toBytes(20))
            .evictionType(EvictionType.MEMORY)
            .storageType(StorageType.OFF_HEAP);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(builder);
      AdvancedCache<Object, Object> cache = manager.getCache().getAdvancedCache();

      cache.put("lifespan", "value", 1, TimeUnit.MINUTES);
      cache.put("both", "value", 1, TimeUnit.MINUTES, 1, TimeUnit.MINUTES);
      cache.put("maxidle", "value", -1, TimeUnit.MINUTES, 1, TimeUnit.MINUTES);

      OffHeapMemoryAllocator allocator = cache.getComponentRegistry().getComponent(
            OffHeapMemoryAllocator.class);
      BoundedOffHeapDataContainer container = (BoundedOffHeapDataContainer) getContainer(cache);
      assertEquals(allocator.getAllocatedAmount(), container.currentSize);

      cache.clear();

      assertEquals(allocator.getAllocatedAmount(), container.currentSize);
   }

   public void testAllocatedAmountEqualWithVersionAndExpiration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory()
            .size(MemoryUnit.MEGABYTES.toBytes(20))
            .evictionType(EvictionType.MEMORY)
            .storageType(StorageType.OFF_HEAP);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(builder);
      AdvancedCache<Object, Object> cache = manager.getCache().getAdvancedCache();

      cache.put("lifespan", 2, new EmbeddedMetadata.Builder()
            .lifespan(1, TimeUnit.MINUTES)
            .version(new NumericVersion(23)).build());
      cache.put("both", 2, new EmbeddedMetadata.Builder()
            .lifespan(1, TimeUnit.MINUTES)
            .maxIdle(1, TimeUnit.MINUTES)
            .version(new NumericVersion(23)).build());
      cache.put("maxidle", 2, new EmbeddedMetadata.Builder()
            .maxIdle(1, TimeUnit.MINUTES)
            .version(new NumericVersion(23)).build());

      OffHeapMemoryAllocator allocator = cache.getComponentRegistry().getComponent(
            OffHeapMemoryAllocator.class);
      BoundedOffHeapDataContainer container = (BoundedOffHeapDataContainer) getContainer(cache);
      assertEquals(allocator.getAllocatedAmount(), container.currentSize);

      cache.clear();

      assertEquals(allocator.getAllocatedAmount(), container.currentSize);
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testAddressCountTooLargeAfterRounding() {
      int addressCount = 3;
      // 30 is more than 3 * 8, but addressCount has to be rounded up so this will not be enough
      long bytes = 30;
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory()
            .size(bytes)
            .evictionType(EvictionType.MEMORY)
            .storageType(StorageType.OFF_HEAP)
            .addressCount(addressCount);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(builder);
   }
}
