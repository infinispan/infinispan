package org.infinispan.container.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.commons.util.IntSets;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "container.impl.DefaultSegmentedDataContainerSizeTest")
public class DefaultSegmentedDataContainerSizeTest extends AbstractInfinispanTest {

   public void testSizeIncludingExpiredWithMoreSegmentsThanTheContainerHas() {
      DefaultSegmentedDataContainer<String, String> container =
            new DefaultSegmentedDataContainer<>(PeekableTouchableContainerMap::new, 1);
      PeekableTouchableContainerMap<String, String> map = new PeekableTouchableContainerMap<>();
      map.put("key-0", new ImmortalCacheEntry("key-0", "value-0"));
      map.put("key-1", new ImmortalCacheEntry("key-1", "value-1"));
      container.maps.set(0, map);

      assertEquals(2, container.sizeIncludingExpired());
      // The segments of the cache configuration, as used for the cache statistics
      assertEquals(2, container.sizeIncludingExpired(IntSets.immutableRangeSet(256)));
   }

   public void testSizeIncludingExpiredOfAContainerWithoutMaps() {
      DefaultSegmentedDataContainer<String, String> container =
            new DefaultSegmentedDataContainer<>(PeekableTouchableContainerMap::new, 4);

      assertEquals(0, container.sizeIncludingExpired(IntSets.immutableRangeSet(256)));
   }
}
