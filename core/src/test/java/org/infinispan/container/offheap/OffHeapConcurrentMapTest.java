package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalEntryFactoryImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "stress", testName = "container.offheap.OffHeapConcurrentMapTest")
public class OffHeapConcurrentMapTest {
   private OffHeapConcurrentMap map;
   private WrappedByteArray valueByteArray = new WrappedByteArray(new byte[] { 0, 1, 2, 3, 4, 5 });

   private static final int RESIZE_LIMITATION = OffHeapConcurrentMap.computeThreshold(OffHeapConcurrentMap.INITIAL_SIZE);

   @BeforeMethod
   void initializeMap() {
      OffHeapMemoryAllocator allocator = new UnpooledOffHeapMemoryAllocator();
      OffHeapEntryFactoryImpl offHeapEntryFactory = new OffHeapEntryFactoryImpl();
      offHeapEntryFactory.allocator = allocator;
      offHeapEntryFactory.internalEntryFactory = new InternalEntryFactoryImpl();
      offHeapEntryFactory.configuration = new ConfigurationBuilder().build();
      offHeapEntryFactory.start();

      map = new OffHeapConcurrentMap(allocator, offHeapEntryFactory, null);
   }

   @AfterMethod
   void afterMethod() {
      if (map != null) {
         map.close();
      }
   }

   private Set<WrappedBytes> insertUpToResizeLimitation() {
      Set<WrappedBytes> keys = new HashSet<>();
      for (int i = 0; i < RESIZE_LIMITATION - 1; ++i) {
         assertTrue(keys.add(putInMap(map, valueByteArray)));
      }
      return keys;
   }

   public void testIterationStartedWithResize1() {
      Set<WrappedBytes> expectedKeys = insertUpToResizeLimitation();

      Set<WrappedBytes> results = new HashSet<>();

      Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iterator = map.values().iterator();
      assertTrue(iterator.hasNext());
      assertTrue(results.add(iterator.next().getKey()));

      // We can't guarantee this value will be in results as we don't know which bucket it mapped to
      putInMap(map, valueByteArray);

      while (iterator.hasNext()) {
         assertTrue(results.add(iterator.next().getKey()));
      }

      for (WrappedBytes expected : expectedKeys) {
         assertTrue("Results didn't contain: " + expected, results.contains(expected));
      }
   }

   public void testIterationStartedWithTwoResizes() {
      Set<WrappedBytes> expectedKeys = insertUpToResizeLimitation();

      Set<Object> results = new HashSet<>();

      Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iterator = map.values().iterator();
      assertTrue(iterator.hasNext());
      assertTrue(results.add(iterator.next().getKey()));

      // Now cause a first and second
      for (int i = 0; i < RESIZE_LIMITATION + 2; ++i) {
         putInMap(map, valueByteArray);
      }

      while (iterator.hasNext()) {
         assertTrue(results.add(iterator.next().getKey()));
      }

      for (WrappedBytes expected : expectedKeys) {
         assertTrue("Results didn't contain: " + expected, results.contains(expected));
      }
   }

   public void testIterationAfterAResize() {
      insertUpToResizeLimitation();

      // Forces resize before iteration
      putInMap(map, valueByteArray);

      int entriesFound = 0;

      for (InternalCacheEntry<WrappedBytes, WrappedBytes> value : map.values()) {
         entriesFound++;
      }

      assertEquals(RESIZE_LIMITATION, entriesFound);
   }

   public void testIterationCreatedButNotUsedUntilAfterAResize() {

      insertUpToResizeLimitation();

      Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iter = map.values().iterator();

      // Forces resize before iteration
      putInMap(map, valueByteArray);

      int entriesFound = 0;

      while (iter.hasNext()) {
         InternalCacheEntry<WrappedBytes, WrappedBytes> ice = iter.next();
         assertNotNull(ice);
         entriesFound++;
      }

      assertEquals(RESIZE_LIMITATION, entriesFound);
   }

   WrappedBytes putInMap(OffHeapConcurrentMap map, WrappedBytes value) {
      InternalCacheEntry<WrappedBytes, WrappedBytes> ice;
      WrappedBytes key;
      do {
         key = randomBytes();
         ice = map.put(key, new ImmortalCacheEntry(key, value));
      } while (ice != null);
      return key;
   }

   WrappedBytes randomBytes() {
      byte[] randomBytes = new byte[16];
      ThreadLocalRandom.current().nextBytes(randomBytes);
      return new WrappedByteArray(randomBytes);
   }
}
