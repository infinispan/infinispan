package org.horizon.container;

import org.horizon.container.entries.ImmortalCacheEntry;
import org.horizon.container.entries.InternalCacheEntry;
import org.horizon.container.entries.MortalCacheEntry;
import org.horizon.container.entries.TransientCacheEntry;
import org.horizon.container.entries.TransientMortalCacheEntry;
import org.testng.annotations.Test;

import java.util.Random;

@Test(groups = "unit", testName = "container.FIFODataContainerTest")
public class FIFODataContainerTest extends SimpleDataContainerTest {

   @Override
   protected DataContainer createContainer() {
      return new FIFODataContainer();
   }

   public void testOrdering() {
      long lifespan = 600000;
      long idle = 600000;
      for (int i=0; i<10; i++) dc.put("k"+i, "value", -1, -1);
      for (int i=10; i<20; i++) dc.put("k"+i, "value", lifespan, -1);
      for (int i=20; i<30; i++) dc.put("k"+i, "value", -1, idle);
      for (int i=30; i<40; i++) dc.put("k"+i, "value", lifespan, idle);

      // random visits
      Random r = new Random();
      for (int i=0; i<100; i++) dc.get("k"+r.nextInt(40));

      // ensure order is maintained.
      int i=0;
      for (InternalCacheEntry ice: dc) {
         assert ice.getKey().equals("k" + i);
         if (i < 10) assert ice instanceof ImmortalCacheEntry;
         if (i >= 10 && i < 20) assert ice instanceof MortalCacheEntry;
         if (i >= 20 && i < 30) assert ice instanceof TransientCacheEntry;
         if (i >= 30 && i < 40) assert ice instanceof TransientMortalCacheEntry;
         i++;
      }
   }
}
