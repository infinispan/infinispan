package org.infinispan.atomic;

import java.util.Map;

public class AtomicHashMapTestAssertions {

   public static void assertIsEmpty(Map map) {
      assert map.size() == 0;
      assert map.get("blah") == null;
      assert !map.containsKey("blah");
   }

   public static void assertIsEmptyMap(AtomicMapCache cache, Object key) {
      assertIsEmpty(cache.getAtomicMap(key));
   }
}
