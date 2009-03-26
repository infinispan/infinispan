package org.horizon.container;

import org.horizon.container.entries.ImmortalCacheEntry;
import org.horizon.container.entries.InternalCacheEntry;
import org.horizon.container.entries.MortalCacheEntry;
import org.horizon.container.entries.TransientCacheEntry;
import org.horizon.container.entries.TransientMortalCacheEntry;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "container.LRUDataContainerTest")
public class LRUDataContainerTest extends SimpleDataContainerTest {
   @Override
   protected DataContainer createContainer() {
      return new LRUDataContainer();
   }

   public void testOrdering() {
      long lifespan = 600000;
      long idle = 600000;
      for (int i=0; i<10; i++) dc.put(i, "value", -1, -1);
      for (int i=10; i<20; i++) dc.put(i, "value", lifespan, -1);
      for (int i=20; i<30; i++) dc.put(i, "value", -1, idle);
      for (int i=30; i<40; i++) dc.put(i, "value", lifespan, idle);

      // Visit all ODD numbered elements
      for (int i=0; i<40; i++) {
         if (i%2 == 1) dc.get(i);
      }

      // ensure order is maintained.  The first 20 elements should be EVEN.
      int i=0;
      for (InternalCacheEntry ice: dc) {
         Integer key = (Integer) ice.getKey();
         if (i<20)
            assert key % 2 == 0;
         else
            assert key % 2 == 1;

         if (key < 10) assert ice instanceof ImmortalCacheEntry;
         if (key >= 10 && key < 20) assert ice instanceof MortalCacheEntry;
         if (key >= 20 && key < 30) assert ice instanceof TransientCacheEntry;
         if (key >= 30 && key < 40) assert ice instanceof TransientMortalCacheEntry;
         i++;
      }
   }
}
