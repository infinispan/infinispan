package org.infinispan.commons.marshall;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/**
 * Ensures there are no duplicate Id values in {@link ProtoStreamTypeIds}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class ProtoStreamTypeIdsUniquenessTest {
   @Test
   public void testIdUniqueness() throws Exception {
      Class clazz = ProtoStreamTypeIds.class;
      Field[] fields = clazz.getFields();
      Set<Integer> messageIds = new HashSet<>();
      Set<Integer> lowerBounds = new HashSet<>();
      for (Field f : fields) {
         if (f.getName().endsWith("_LOWER_BOUND"))
            assertTrue(f.getName(), lowerBounds.add(f.getInt(clazz)));
         else
            assertTrue(f.getName(), messageIds.add(f.getInt(clazz)));
      }
      assertTrue(!messageIds.isEmpty());
      assertTrue(!lowerBounds.isEmpty());
      assertEquals(fields.length - lowerBounds.size(), messageIds.size());
      assertEquals(fields.length - messageIds.size(), lowerBounds.size());
   }
}
