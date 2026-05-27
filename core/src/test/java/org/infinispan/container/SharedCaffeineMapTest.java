package org.infinispan.container;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.impl.InternalEntryFactoryImpl;
import org.infinispan.container.impl.SharedCaffeineMap;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.function.TriConsumer;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "container.SharedCaffeineMap")
public class SharedCaffeineMapTest extends AbstractInfinispanTest {
   private ControlledTimeService timeService;

   private BasicComponentRegistry basicComponentRegistry;
   private boolean isDistributed;

   public SharedCaffeineMapTest(boolean isDistributed) {
      this.isDistributed = isDistributed;
   }

   @BeforeMethod
   public void before() {
      InternalEntryFactory internalEntryFactory = new InternalEntryFactoryImpl();
      timeService = new ControlledTimeService();
      TestingUtil.inject(internalEntryFactory, timeService);
      KeyPartitioner keyPartitioner = key -> {
         if (key instanceof String s)
            return s.charAt(s.length() - 1) - 48;
         throw new IllegalArgumentException();
      };

      basicComponentRegistry = Mockito.mock(BasicComponentRegistry.class);
      Mockito.doAnswer(i -> {
         Object arg = i.getArgument(0);
         Configuration configuration = Mockito.mock(Configuration.class, Mockito.RETURNS_DEEP_STUBS);
         Mockito.when(configuration.clustering().cacheMode().isDistributed()).thenReturn(isDistributed);
         TestingUtil.inject(arg, timeService, internalEntryFactory, configuration, keyPartitioner);
         return null;
      }).when(basicComponentRegistry).wireDependencies(Mockito.any(), Mockito.anyBoolean());
   }

   <K, V> SharedCaffeineMap<K, V> newMap(int evictionSize, boolean memoryStorage) {
      SharedCaffeineMap<K, V> map = new SharedCaffeineMap<>(evictionSize, memoryStorage);
      return map;
   }

   @Factory
   public static Object[] factory() {
      return new Object[] {
            new SharedCaffeineMapTest(true),
            new SharedCaffeineMapTest(false),
      };
   }

   @Override
   protected String parameters() {
      return "[" + isDistributed + "]";
   }

   @Test
   public void simpleSizeTest() {
      int evictionSize = 10;
      SharedCaffeineMap<String, String> container = newMap(evictionSize, false);

      InternalDataContainer<String, String> fooContainer = container.newContainer("foo", basicComponentRegistry, 32);

      for (int i = 0; i < evictionSize; ++i) {
         fooContainer.put(i, "foo-" + i, "bar-" + i, null, null, -1, -1);
      }

      assertEquals(new ImmortalCacheEntry("foo-2", "bar-2"), fooContainer.get("foo-2"));

      assertEquals(evictionSize, fooContainer.size());

      InternalDataContainer<String, String> barContainer = container.newContainer("bar", basicComponentRegistry, 10);

      for (int i = 0; i < evictionSize; ++i) {
         barContainer.put(i, "bar-" + i, "foo", null, null, -1, -1);
      }

      int totalSize = fooContainer.size() + barContainer.size();
      assertEquals(10, totalSize);
   }

   @Test
   public void memorySizeWithFourCachesSameKeysTest() {
      int evictionMemorySize = 10_000;

      SharedCaffeineMap<String, String> container = newMap(evictionMemorySize, true);

      InternalDataContainer<String, String> container1 = container.newContainer("container1", basicComponentRegistry, 10);
      InternalDataContainer<String, String> container2 = container.newContainer("container2", basicComponentRegistry, 98);
      InternalDataContainer<String, String> container3 = container.newContainer("container3", basicComponentRegistry, 23);
      InternalDataContainer<String, String> container4 = container.newContainer("container4", basicComponentRegistry, 62);


      for (int i = 0; i < 20; ++i) {
         container1.put(0, "key-" + i, "value1-" + i, null, null, -1, -1);
         container2.put(0, "key-" + i, "value2-" + i, null, null, -1, -1);
         container3.put(0, "key-" + i, "value3-" + i, null, null, -1, -1);
         container4.put(0, "key-" + i, "value4-" + i, null, null, -1, -1);
      }

      int container1Size = container1.size();
      int container2Size = container2.size();
      int container3Size = container3.size();
      int container4Size = container4.size();

      if (container1Size + container2Size + container3Size + container4Size == 80) {
         fail("Container size should have been less than 80 was: " + container1Size + ", " + container2Size + ", " + container3Size + ", " + container4Size);
      }
   }

   @Test
   public void testResize() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);
      for (int i = 0; i < 5; i++) {
         subContainer.put(0, "key-" + i, "value" + i, null, null, -1, -1);
      }
      assertEquals(5, subContainer.size());
      subContainer.resize(20);
      assertEquals(20, subContainer.capacity());
   }

   @Test
   public void testEvictionSize() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 15);
      for (int i = 0; i < 5; i++) {
         subContainer.put("key-" + i, "value" + i, null);
      }
      assertEquals(5, subContainer.size());
      assertEquals(5, subContainer.evictionSize());

      for (int i = 5; i < 15; i++) {
         subContainer.put("key-" + i, "value" + i, null);
      }
      assertEquals(10, subContainer.size());
      assertEquals(10, subContainer.evictionSize());
   }

   @Test
   public void testCapacity() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);
      for (int i = 0; i < 5; i++) {
         subContainer.put(0, "key-" + i, "value" + i, null, null, -1, -1);
      }
      assertEquals(10, subContainer.capacity());
   }

   @Test
   public void testPeek() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);
      subContainer.put(1, "key-1", "value1", null, null, -1, -1);
      assertEquals(new ImmortalCacheEntry("key-1", "value1"), subContainer.peek("key-1"));
      assertEquals(1, subContainer.size()); // Peek should not affect size
   }

   @Test
   public void testClear() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);
      for (int i = 0; i < 5; i++) {
         subContainer.put(0, "key-" + i, "value" + i, null, null, -1, -1);
      }
      assertEquals(5, subContainer.size());
      subContainer.clear();
      assertEquals(0, subContainer.size());
   }

   @Test
   public void testContainsKey() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);
      subContainer.put(1, "key-1", "value1", null, null, -1, -1);
      assertTrue(subContainer.containsKey("key-1"));
      assertFalse(subContainer.containsKey("key-2"));
   }

   @Test
   public void testRemove() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);
      subContainer.put(1, "key-1", "value1", null, null, -1, -1);
      assertEquals(1, subContainer.size());
      subContainer.remove("key-1");
      assertEquals(0, subContainer.size());
      assertFalse(subContainer.containsKey("key-1"));
   }

   @Test
   public void testRemoveSegments() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);

      // Populate with entries across multiple segments
      subContainer.put(0, "key-0", "value-0", null, null, -1, -1);
      subContainer.put(1, "key-1", "value-1", null, null, -1, -1);
      subContainer.put(2, "key-2", "value-2", null, null, -1, -1);
      subContainer.put(3, "key-3", "value-3", null, null, -1, -1);

      assertEquals(4, subContainer.size());

      // Remove segments 1 and 3
      IntSet segmentsToRemove = IntSets.mutableSet(1, 3);
      subContainer.removeSegments(segmentsToRemove);

      assertEquals(2, subContainer.size());
      assertTrue(subContainer.containsKey("key-0"));
      assertFalse(subContainer.containsKey("key-1"));
      assertTrue(subContainer.containsKey("key-2"));
      assertFalse(subContainer.containsKey("key-3"));
   }

   @Test
   public void testClearWithSegments() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);

      // Populate with entries across multiple segments
      subContainer.put(0, "key-0", "value-0", null, null, -1, -1);
      subContainer.put(1, "key-1", "value-1", null, null, -1, -1);
      subContainer.put(2, "key-2", "value-2", null, null, -1, -1);
      subContainer.put(3, "key-3", "value-3", null, null, -1, -1);

      assertEquals(4, subContainer.size());

      // Clear segments 0 and 2
      IntSet segmentsToClear = IntSets.mutableSet(0, 2);
      subContainer.clear(segmentsToClear);

      assertEquals(2, subContainer.size());
      assertFalse(subContainer.containsKey("key-0"));
      assertTrue(subContainer.containsKey("key-1"));
      assertFalse(subContainer.containsKey("key-2"));
      assertTrue(subContainer.containsKey("key-3"));
   }

   @Test
   public void testNonSegmentedBasicOperations() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 1);

      subContainer.put("key-0", "value-0", null);
      subContainer.put("key-1", "value-1", null);
      subContainer.put("key-2", "value-2", null);

      assertEquals(3, subContainer.size());
      assertEquals(3, subContainer.sizeIncludingExpired());
      assertEquals(new ImmortalCacheEntry("key-1", "value-1"), subContainer.get("key-1"));
   }

   @Test
   public void testNonSegmentedSizeIncludingExpiredWithLargeIntSet() {
      SharedCaffeineMap<String, String> container = newMap(100, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 1);

      for (int i = 0; i < 5; i++) {
         subContainer.put("key-" + i, "value-" + i, null);
      }

      assertEquals(5, subContainer.sizeIncludingExpired());
      // This is the exact scenario from the bug - calling sizeIncludingExpired with a large IntSet
      // should not throw ArrayIndexOutOfBoundsException
      IntSet largeSegmentSet = IntSets.immutableRangeSet(256);
      assertEquals(5, subContainer.sizeIncludingExpired(largeSegmentSet));
   }

   @Test
   public void testNonSegmentedEvictionAcrossContainers() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> first = container.newContainer("first", basicComponentRegistry, 1);
      InternalDataContainer<String, String> second = container.newContainer("second", basicComponentRegistry, 1);

      // Fill first container to the limit
      for (int i = 0; i < 10; i++) {
         first.put("key-" + i, "value-" + i, null);
      }
      assertEquals(10, first.size());

      // Inserting into second must evict entries from first since the shared container is full
      for (int i = 0; i < 10; i++) {
         second.put("key-" + i, "value-" + i, null);
      }
      assertTrue("Inserts into second should have evicted from first, but first has " + first.size(),
            first.size() < 10);
      int totalSize = first.size() + second.size();
      assertTrue("Total size should be at most 10 but was " + totalSize, totalSize <= 10);

      // Now insert more into first with new keys to cause evictions from second
      int secondBefore = second.size();
      for (int i = 10; i < 20; i++) {
         first.put("key-" + i, "value-" + i, null);
      }
      assertTrue("Inserts into first should have evicted from second, but second has " + second.size()
            + " (was " + secondBefore + ")", second.size() < secondBefore);
      totalSize = first.size() + second.size();
      assertTrue("Total size should be at most 10 but was " + totalSize, totalSize <= 10);
   }

   @Test
   public void testNonSegmentedClear() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 1);

      for (int i = 0; i < 5; i++) {
         subContainer.put("key-" + i, "value-" + i, null);
      }

      assertEquals(5, subContainer.size());
      subContainer.clear();
      assertEquals(0, subContainer.size());
   }

   @Test
   public void testNonSegmentedClearNotAffectOther() {
      SharedCaffeineMap<String, String> container = newMap(20, false);
      InternalDataContainer<String, String> first = container.newContainer("first", basicComponentRegistry, 1);
      InternalDataContainer<String, String> second = container.newContainer("second", basicComponentRegistry, 1);

      for (int i = 0; i < 5; i++) {
         first.put("key-" + i, "value-" + i, null);
         second.put("key-" + i, "value-" + i, null);
      }

      assertEquals(10, container.getCache().estimatedSize());
      assertEquals(5, first.size());
      assertEquals(5, second.size());

      first.clear();

      assertEquals(5, container.getCache().estimatedSize());
      assertEquals(0, first.size());
      assertEquals(5, second.size());
   }

   @Test
   public void testNonSegmentedPeekAndContainsKey() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 1);

      subContainer.put("key-1", "value-1", null);

      assertEquals(new ImmortalCacheEntry("key-1", "value-1"), subContainer.peek("key-1"));
      assertTrue(subContainer.containsKey("key-1"));
      assertFalse(subContainer.containsKey("key-2"));
   }

   @Test
   public void testNonSegmentedCapacityAndResize() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 1);

      assertEquals(10, subContainer.capacity());
      subContainer.resize(20);
      assertEquals(20, subContainer.capacity());
   }

   @Test
   public void testNonSegmentedRemove() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 1);

      subContainer.put("key-1", "value-1", null);
      assertEquals(1, subContainer.size());

      subContainer.remove("key-1");
      assertEquals(0, subContainer.size());
      assertFalse(subContainer.containsKey("key-1"));
   }

   @Test
   public void testMixedSegmentedAndNonSegmentedSharingContainer() {
      SharedCaffeineMap<String, String> container = newMap(100, false);
      InternalDataContainer<String, String> segmented = container.newContainer("segmented", basicComponentRegistry, 5);
      InternalDataContainer<String, String> nonSegmented = container.newContainer("non-segmented", basicComponentRegistry, 1);

      for (int i = 0; i < 5; i++) {
         segmented.put(i, "seg-key-" + i, "seg-value-" + i, null, null, -1, -1);
         nonSegmented.put("nonseg-key-" + i, "nonseg-value-" + i, null);
      }

      assertEquals(5, segmented.size());
      assertEquals(5, nonSegmented.size());
      assertEquals(10, container.getCache().estimatedSize());

      // Clearing the non-segmented container should not affect the segmented one
      nonSegmented.clear();
      assertEquals(0, nonSegmented.size());
      assertEquals(5, segmented.size());
      assertEquals(5, container.getCache().estimatedSize());

      // And vice versa
      for (int i = 0; i < 3; i++) {
         nonSegmented.put("nonseg-key-" + i, "nonseg-value-" + i, null);
      }
      segmented.clear();
      assertEquals(0, segmented.size());
      assertEquals(3, nonSegmented.size());
      assertEquals(3, container.getCache().estimatedSize());
   }

   private void twoContainerSetup(TriConsumer<SharedCaffeineMap<String, String>, InternalDataContainer<String, String>,
         InternalDataContainer<String, String>> verifications) {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      InternalDataContainer<String, String> firstContainer = container.newContainer("first", basicComponentRegistry, 5);
      InternalDataContainer<String, String> secondContainer = container.newContainer("second", basicComponentRegistry, 5);

      for (int i = 0; i < 5; ++i) {
         firstContainer.put(i, "key-" + i, "value-" + i, null, null, -1, -1);
         secondContainer.put(i, "key-" + i, "value-" + i, null, null, -1, -1);
      }

      assertEquals(10, container.getCache().estimatedSize());

      assertEquals(5, firstContainer.size());
      assertEquals(5, secondContainer.size());

      verifications.accept(container, firstContainer, secondContainer);
   }

   @Test
   public void testClearNotAffectOther() {
      twoContainerSetup((container, firstContainer, secondContainer) -> {
         firstContainer.clear();

         assertEquals(5, container.getCache().estimatedSize());

         assertEquals(0, firstContainer.size());
         assertEquals(5, secondContainer.size());
      });
   }

   @Test
   public void testClearSegmentNotAffectOther() {
      twoContainerSetup((container, firstContainer, secondContainer) -> {
         firstContainer.clear(IntSets.mutableSet(2, 4));

         assertEquals(8, container.getCache().estimatedSize());

         assertEquals(3, firstContainer.size());
         assertEquals(5, secondContainer.size());
      });
   }

   @Test
   public void testRemoveSegmentNotAffectOther() {
      twoContainerSetup((container, firstContainer, secondContainer) -> {
         firstContainer.removeSegments(IntSets.mutableSet(2, 4));

         assertEquals(8, container.getCache().estimatedSize());

         assertEquals(3, firstContainer.size());
         assertEquals(5, secondContainer.size());
      });
   }
}
