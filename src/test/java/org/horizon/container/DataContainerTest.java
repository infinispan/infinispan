package org.horizon.container;

import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

@Test(groups = "unit", testName = "container.DataContainerTest")
public class DataContainerTest {
   public void testExpiredData() throws InterruptedException {
      DataContainer dc = new UnsortedDataContainer();
      dc.put("k", "v", -1);
      Thread.sleep(100);

      assert dc.getModifiedTimestamp("k") <= System.currentTimeMillis();

      dc.get("k");
      assert dc.purgeExpiredEntries().isEmpty();

      dc.put("k", "v", 0);
      Thread.sleep(100);
      assert dc.size() == 1;
      assert dc.getModifiedTimestamp("k") <= System.currentTimeMillis();

      assert dc.get("k") == null;
      assert dc.size() == 0;

      dc.put("k", "v", 0);
      Thread.sleep(100);
      assert dc.size() == 1;
      assert dc.getModifiedTimestamp("k") <= System.currentTimeMillis();

      assert dc.purgeExpiredEntries().contains("k");
      assert dc.size() == 0;
   }

   public void testExpirableToImmortal() {
      UnsortedDataContainer dc = new UnsortedDataContainer();
      dc.put("k", "v", 6000000);
      assert dc.containsKey("k");
      assert !dc.immortalData.containsKey("k");
      assert dc.expirableData.containsKey("k");

      dc.put("k", "v2", -1);
      assert dc.containsKey("k");
      assert dc.immortalData.containsKey("k");
      assert !dc.expirableData.containsKey("k");

      dc.put("k", "v3", 700000);
      assert dc.containsKey("k");
      assert !dc.immortalData.containsKey("k");
      assert dc.expirableData.containsKey("k");
   }

   public void testKeySet() {
      UnsortedDataContainer dc = new UnsortedDataContainer();
      dc.put("k1", "v", 6000000);
      dc.put("k2", "v", -1);

      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");

      for (Object o : dc.keySet()) {
         assert expected.remove(o);
      }

      assert expected.isEmpty();
   }
}
