package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
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
}
