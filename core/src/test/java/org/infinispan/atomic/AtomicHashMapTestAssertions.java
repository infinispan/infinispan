package org.infinispan.atomic;

import org.infinispan.Cache;

import java.util.Map;

public class AtomicHashMapTestAssertions {

   public static void assertIsEmpty(Map map) {
      assert map.size() == 0;
      assert map.get("blah") == null;
      assert !map.containsKey("blah");
   }

   public static void assertIsEmptyMap(Cache<?, ?> cache, Object key) {
      assertIsEmpty(AtomicMapLookup.getAtomicMap(cache, key));
   }
}
