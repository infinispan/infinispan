package org.infinispan.atomic;

import org.infinispan.atomic.impl.AtomicHashMap;
import org.infinispan.atomic.impl.AtomicHashMapDelta;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "atomic.AtomicHashMapTest")
public class AtomicHashMapTest extends AbstractInfinispanTest {
   public void testDeltasWithEmptyMap() {
      AtomicHashMap m = new AtomicHashMap();
      Delta d = m.delta();
      assert d instanceof NullDelta;

      AtomicHashMap newMap = new AtomicHashMap();
      newMap.initForWriting();
      newMap.put("k", "v");
      newMap = (AtomicHashMap) d.merge(newMap);
      assert newMap.containsKey("k");
      assert newMap.size() == 1;

      newMap = (AtomicHashMap) d.merge(null);
      assert newMap.isEmpty();
   }

   public void testDeltasWithNoChanges() {
      AtomicHashMap m = new AtomicHashMap();
      m.initForWriting();
      m.put("k1", "v1");
      m.commit();
      assert m.size() == 1;
      Delta d = m.delta();
      assert d instanceof NullDelta;

      AtomicHashMap newMap = new AtomicHashMap();
      newMap.initForWriting();
      newMap.put("k", "v");
      newMap = (AtomicHashMap) d.merge(newMap);
      assert newMap.containsKey("k");
      assert newMap.size() == 1;

      newMap = (AtomicHashMap) d.merge(null);
      assert newMap.isEmpty();
   }

   public void testDeltasWithRepeatedChanges() {
      AtomicHashMap m = new AtomicHashMap();
      m.initForWriting();
      m.put("k1", "v1");
      m.put("k1", "v2");
      m.put("k1", "v3");
      assert m.size() == 1;
      AtomicHashMapDelta d = (AtomicHashMapDelta) m.delta();
      assert d.getChangeLogSize() != 0;

      AtomicHashMap newMap = new AtomicHashMap();
      newMap.initForWriting();
      newMap.put("k1", "v4");
      newMap = (AtomicHashMap) d.merge(newMap);
      assert newMap.containsKey("k1");
      assert newMap.get("k1").equals("v3");
      assert newMap.size() == 1;

      newMap = (AtomicHashMap) d.merge(null);
      assert newMap.containsKey("k1");
      assert newMap.get("k1").equals("v3");
      assert newMap.size() == 1;
   }
}
