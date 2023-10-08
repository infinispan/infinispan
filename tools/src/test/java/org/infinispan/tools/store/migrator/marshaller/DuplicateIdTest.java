package org.infinispan.tools.store.migrator.marshaller;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.tools.store.migrator.marshaller.common.Ids;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "marshall.DuplicateIdTest")
public class DuplicateIdTest extends AbstractInfinispanTest {
   public void testDuplicateMarshallerIds() throws Exception {
      Class idHolder = Ids.class;
      Map<Integer, Set<String>> dupes = new HashMap<>();
      for (Field f : idHolder.getDeclaredFields()) {
         if (Modifier.isStatic(f.getModifiers()) && Modifier.isFinal(f.getModifiers()) && f.getType() == int.class) {
            int val = (Integer) f.get(null);
            Set<String> names = dupes.get(val);
            if (names == null) names = new HashSet<String>();
            names.add(f.getName());
            dupes.put(val, names);
         }
      }

      int largest = 0;
      for (Map.Entry<Integer, Set<String>> e : dupes.entrySet()) {
         assert e.getValue().size() == 1 : "ID " + e.getKey() + " is duplicated by fields " + e.getValue();
         largest = Math.max(largest, e.getKey());
      }

      log.trace("Next available ID is " + (largest + 1));
   }
}
