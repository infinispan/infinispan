package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.util.ByteQuantity;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.factories.ComponentRegistry;
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
            // Only allocate enough for address count and 1 byte
            .maxSize(UnpooledOffHeapMemoryAllocator.estimateSizeOverhead((OffHeapConcurrentMap.INITIAL_SIZE << 3)) + 1)
            .storage(StorageType.OFF_HEAP);
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
            .maxSize(ByteQuantity.Unit.MB.toBytes(20))
            .storage(StorageType.OFF_HEAP);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(builder);
      AdvancedCache<Object, Object> cache = manager.getCache().getAdvancedCache();

      OffHeapMemoryAllocator allocator =  ComponentRegistry.componentOf(cache, OffHeapMemoryAllocator.class);
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
            .maxSize(ByteQuantity.Unit.MB.toBytes(20))
            .storage(StorageType.OFF_HEAP);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(builder);
      AdvancedCache<Object, Object> cache = manager.getCache().getAdvancedCache();

      cache.put(1, 2, new EmbeddedMetadata.Builder().version(new NumericVersion(23)).build());

      OffHeapMemoryAllocator allocator =  ComponentRegistry.componentOf(cache, OffHeapMemoryAllocator.class);
      BoundedOffHeapDataContainer container = (BoundedOffHeapDataContainer) getContainer(cache);
      assertEquals(allocator.getAllocatedAmount(), container.currentSize);

      cache.clear();

      assertEquals(allocator.getAllocatedAmount(), container.currentSize);
   }

   public void testAllocatedAmountEqualWithExpiration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory()
            .maxSize(ByteQuantity.Unit.MB.toBytes(20))
            .storage(StorageType.OFF_HEAP);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(builder);
      AdvancedCache<Object, Object> cache = manager.getCache().getAdvancedCache();

      cache.put("lifespan", "value", 1, TimeUnit.MINUTES);
      cache.put("both", "value", 1, TimeUnit.MINUTES, 1, TimeUnit.MINUTES);
      cache.put("maxidle", "value", -1, TimeUnit.MINUTES, 1, TimeUnit.MINUTES);

      OffHeapMemoryAllocator allocator =  ComponentRegistry.componentOf(cache, OffHeapMemoryAllocator.class);
      BoundedOffHeapDataContainer container = (BoundedOffHeapDataContainer) getContainer(cache);
      assertEquals(allocator.getAllocatedAmount(), container.currentSize);

      cache.clear();

      assertEquals(allocator.getAllocatedAmount(), container.currentSize);
   }

   public void testAllocatedAmountEqualWithVersionAndExpiration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory()
            .maxSize(ByteQuantity.Unit.MB.toBytes(20))
            .storage(StorageType.OFF_HEAP);
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

      OffHeapMemoryAllocator allocator =  ComponentRegistry.componentOf(cache, OffHeapMemoryAllocator.class);
      BoundedOffHeapDataContainer container = (BoundedOffHeapDataContainer) getContainer(cache);
      assertEquals(allocator.getAllocatedAmount(), container.currentSize);

      cache.clear();

      assertEquals(allocator.getAllocatedAmount(), container.currentSize);
   }
}
