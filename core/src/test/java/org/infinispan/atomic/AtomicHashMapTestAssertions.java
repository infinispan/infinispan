package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.test.AbstractInfinispanTest;

import java.util.Map;

public class AtomicHashMapTestAssertions extends AbstractInfinispanTest {

   public static void assertIsEmpty(Map map) {
      assert map.isEmpty();
      assert map.get("blah") == null;
      assert !map.containsKey("blah");
   }

   public static void assertIsEmptyMap(Cache<?, ?> cache, Object key) {
      assertIsEmpty(AtomicMapLookup.getAtomicMap(cache, key));
   }
}
