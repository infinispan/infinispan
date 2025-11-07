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
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.impl.InternalEntryFactoryImpl;
import org.infinispan.container.impl.SharedBoundedContainer;
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

      SharedBoundedContainer<String, String> fooContainer = container.newContainer("foo", basicComponentRegistry, 32);

      for (int i = 0; i < evictionSize; ++i) {
         fooContainer.put(i, "foo-" + i, "bar-" + i, null, null, -1, -1);
      }

      assertEquals(new ImmortalCacheEntry("foo-2", "bar-2"), fooContainer.get("foo-2"));

      assertEquals(evictionSize, fooContainer.size());

      SharedBoundedContainer<String, String> barContainer = container.newContainer("bar", basicComponentRegistry, 10);

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

      SharedBoundedContainer<String, String> container1 = container.newContainer("container1", basicComponentRegistry, 10);
      SharedBoundedContainer<String, String> container2 = container.newContainer("container2", basicComponentRegistry, 98);
      SharedBoundedContainer<String, String> container3 = container.newContainer("container3", basicComponentRegistry, 23);
      SharedBoundedContainer<String, String> container4 = container.newContainer("container4", basicComponentRegistry, 62);


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
      SharedBoundedContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);
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
      SharedBoundedContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 15);
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
      SharedBoundedContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);
      for (int i = 0; i < 5; i++) {
         subContainer.put(0, "key-" + i, "value" + i, null, null, -1, -1);
      }
      assertEquals(10, subContainer.capacity());
   }

   @Test
   public void testPeek() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      SharedBoundedContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);
      subContainer.put(1, "key-1", "value1", null, null, -1, -1);
      assertEquals(new ImmortalCacheEntry("key-1", "value1"), subContainer.peek("key-1"));
      assertEquals(1, subContainer.size()); // Peek should not affect size
   }

   @Test
   public void testClear() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      SharedBoundedContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);
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
      SharedBoundedContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);
      subContainer.put(1, "key-1", "value1", null, null, -1, -1);
      assertTrue(subContainer.containsKey("key-1"));
      assertFalse(subContainer.containsKey("key-2"));
   }

   @Test
   public void testRemove() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      SharedBoundedContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);
      subContainer.put(1, "key-1", "value1", null, null, -1, -1);
      assertEquals(1, subContainer.size());
      subContainer.remove("key-1");
      assertEquals(0, subContainer.size());
      assertFalse(subContainer.containsKey("key-1"));
   }

   @Test
   public void testRemoveSegments() {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      SharedBoundedContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);

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
      SharedBoundedContainer<String, String> subContainer = container.newContainer("test", basicComponentRegistry, 5);

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

   private void twoContainerSetup(TriConsumer<SharedCaffeineMap<String, String>, SharedBoundedContainer<String, String>,
         SharedBoundedContainer<String, String>> verifications) {
      SharedCaffeineMap<String, String> container = newMap(10, false);
      SharedBoundedContainer<String, String> firstContainer = container.newContainer("first", basicComponentRegistry, 5);
      SharedBoundedContainer<String, String> secondContainer = container.newContainer("second", basicComponentRegistry, 5);

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
