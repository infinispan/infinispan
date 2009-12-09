package org.infinispan.container;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "container.LRUDataContainerTest")
public class LRUDataContainerTest extends FIFODataContainerTest {
   @Override
   protected DataContainer createContainer() {
      return new LRUSimpleDataContainer(16, 1);
   }

   @Override
   protected Class<? extends InternalCacheEntry> mortaltype() {
      return TransientMortalCacheEntry.class;
   }

   @Override
   protected Class<? extends InternalCacheEntry> immortaltype() {
      return TransientCacheEntry.class;
   }

   @Override
   protected Class<? extends InternalCacheEntry> transienttype() {
      return TransientCacheEntry.class;
   }   

   @Override
   public void testOrdering() {
      long lifespan = 600000;
      long idle = 600000;
      for (int i = 0; i < 10; i++) {
         dc.put(i, "value", -1, -1);
         TestingUtil.sleepThread(10);
      }
      for (int i = 10; i < 20; i++) {
         dc.put(i, "value", lifespan, -1);
         TestingUtil.sleepThread(10);
      }
      for (int i = 20; i < 30; i++) {
         dc.put(i, "value", -1, idle);
         TestingUtil.sleepThread(10);
      }
      for (int i = 30; i < 40; i++) {
         dc.put(i, "value", lifespan, idle);
         TestingUtil.sleepThread(10);
      }

      // Visit all ODD numbered elements
      for (int i = 0; i < 40; i++) {
         if (i % 2 == 1) dc.get(i);
         TestingUtil.sleepThread(10);
      }

      // ensure order is maintained.  The first 20 elements should be EVEN.
      int i = 0;
      for (InternalCacheEntry ice : dc) {
         Integer key = (Integer) ice.getKey();
         if (i < 20)
            assert key % 2 == 0;
         else
            assert key % 2 == 1;

         if (key < 10) assert ice.getClass().equals(immortaltype());
         if (key >= 10 && key < 20) assert ice.getClass().equals(mortaltype());
         if (key >= 20 && key < 30) assert ice.getClass().equals(transienttype()) : "Expected " + transienttype() + " for key " + key + " but was " + ice.getClass();
         if (key >= 30 && key < 40) assert ice instanceof TransientMortalCacheEntry;
         i++;
      }
   }
}