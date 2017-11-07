package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.eviction.EvictionType;
import org.infinispan.manager.EmbeddedCacheManager;
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
            .size(10)
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
      return cache.getDataContainer();
   }

   /**
    * Test to ensure that off heap allocated amount equals the total size used by the container
    */
   public void testAllocatedAmountEqual() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory()
            .size(100)
            .evictionType(EvictionType.MEMORY)
            .storageType(StorageType.OFF_HEAP);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(builder);
      AdvancedCache<Object, Object> cache = manager.getCache().getAdvancedCache();
      cache.put(1, 2);
      OffHeapMemoryAllocator allocator = cache.getComponentRegistry().getComponent(
            OffHeapMemoryAllocator.class);
      BoundedOffHeapDataContainer container = (BoundedOffHeapDataContainer) getContainer(cache);
      assertEquals(allocator.getAllocatedAmount(), container.currentSize);

   }
}
