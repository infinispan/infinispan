package org.infinispan.commons;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.EquivalentLinkedHashMap;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.jgroups.util.Util.assertFalse;
import static org.jgroups.util.Util.assertTrue;
import static org.testng.AssertJUnit.assertEquals;


/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "unit", testName = "commons.EquivalentLinkedHashMapTest")
public class EquivalentLinkedHashMapTest {

   public void testIterationAndSize() {
      EquivalentLinkedHashMap map = new EquivalentLinkedHashMap(16, 0.75f,
                                                                EquivalentLinkedHashMap.IterationOrder.ACCESS_ORDER,
                                                                AnyEquivalence.getInstance(), AnyEquivalence.getInstance());

      map.put("k1","v1");
      map.put("k2","v2");
      map.put("k3","v3");

      Collection values = map.values();
      Iterator iterator = values.iterator();
      assertTrue(iterator.hasNext());
      assertEquals("v1",iterator.next());
      assertTrue(iterator.hasNext());
      assertEquals("v2",iterator.next());
      assertTrue(iterator.hasNext());
      assertEquals("v3",iterator.next());
      assertFalse(iterator.hasNext());

      Set keys = map.keySet();
      iterator = keys.iterator();
      assertTrue(iterator.hasNext());
      assertEquals("k1",iterator.next());
      assertTrue(iterator.hasNext());
      assertEquals("k2",iterator.next());
      assertTrue(iterator.hasNext());
      assertEquals("k3",iterator.next());
      assertFalse(iterator.hasNext());

      Set set = map.entrySet();
      iterator = set.iterator();
      assertTrue(iterator.hasNext());
      Map.Entry next = (Map.Entry) iterator.next();
      assertEquals("k1", next.getKey());
      assertEquals("v1", next.getValue());
      assertTrue(iterator.hasNext());
      next = (Map.Entry) iterator.next();
      assertEquals("k2", next.getKey());
      assertEquals("v2", next.getValue());
      assertTrue(iterator.hasNext());
      next = (Map.Entry) iterator.next();
      assertEquals("k3", next.getKey());
      assertEquals("v3", next.getValue());
      assertFalse(iterator.hasNext());

      assertEquals(map.size(), 3);
      assertEquals(map.keySet().size(), 3);
      assertEquals(map.values().size(), 3);
      assertEquals(map.entrySet().size(), 3);
   }

}
