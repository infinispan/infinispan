package org.horizon.container;

import org.testng.annotations.Test;
import org.horizon.container.entries.InternalCacheEntry;
import org.horizon.container.entries.TransientCacheEntry;
import org.horizon.container.entries.MortalCacheEntry;

import java.util.HashSet;
import java.util.Set;

@Test(groups = "unit", testName = "container.SimpleDataContainerTest")
public class SimpleDataContainerTest {
   public void testExpiredData() throws InterruptedException {
      DataContainer dc = new SimpleDataContainer();
      dc.put("k", "v", -1, 6000000);
      Thread.sleep(100);

      InternalCacheEntry entry = dc.get("k");
      assert entry instanceof TransientCacheEntry;
      assert entry.getLastUsed() <= System.currentTimeMillis();
      long entryLastUsed = entry.getLastUsed();
      Thread.sleep(10);
      entry = dc.get("k");
      assert entry.getLastUsed() > entryLastUsed;
      dc.put("k", "v", -1, 0);
      dc.purge();

      dc.put("k", "v", 6000000, -1);
      Thread.sleep(100);
      assert dc.size() == 1;

      entry = dc.get("k");
      assert entry instanceof MortalCacheEntry;
      assert entry.getCreated() <= System.currentTimeMillis();

      dc.put("k", "v", 0, -1);

      assert dc.get("k") == null;
      assert dc.size() == 0;

      dc.put("k", "v", 0, -1);
      Thread.sleep(100);
      assert dc.size() == 1;
      dc.purge();
      assert dc.size() == 0;
   }

   public void testExpirableToImmortal() {
      SimpleDataContainer dc = new SimpleDataContainer();
      dc.put("k", "v", 6000000, -1);
      assert dc.containsKey("k");
      assert !dc.immortalEntries.containsKey("k");
      assert dc.mortalEntries.containsKey("k");

      dc.put("k", "v2", -1, -1);
      assert dc.containsKey("k");
      assert dc.immortalEntries.containsKey("k");
      assert !dc.mortalEntries.containsKey("k");

      dc.put("k", "v3", -1, 6000000);
      assert dc.containsKey("k");
      assert !dc.immortalEntries.containsKey("k");
      assert dc.mortalEntries.containsKey("k");
   }

   public void testKeySet() {
      SimpleDataContainer dc = new SimpleDataContainer();
      dc.put("k1", "v", 6000000, -1);
      dc.put("k2", "v", -1, -1);
      dc.put("k3", "v", -1, 6000000);
      dc.put("k4", "v", 6000000, 6000000);

      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      expected.add("k4");

      for (Object o : dc.keySet()) assert expected.remove(o);

      assert expected.isEmpty();
   }
}
