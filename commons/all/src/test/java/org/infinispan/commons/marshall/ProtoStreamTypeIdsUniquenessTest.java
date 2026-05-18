package org.infinispan.commons.marshall;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

/**
 * Ensures there are no duplicate Id values in {@link ProtoStreamTypeIds}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class ProtoStreamTypeIdsUniquenessTest {
   @Test
   public void testIdUniqueness() throws Exception {
      Class<?> clazz = ProtoStreamTypeIds.class;
      Field[] fields = clazz.getFields();
      Set<Integer> messageIds = new HashSet<>();
      Set<Integer> lowerBounds = new HashSet<>();
      for (Field f : fields) {
         if (f.getName().endsWith("_LOWER_BOUND"))
            assertTrue(lowerBounds.add(f.getInt(clazz)), f.getName());
         else
            assertTrue(messageIds.add(f.getInt(clazz)), f.getName());
      }
      assertFalse(messageIds.isEmpty());
      assertFalse(lowerBounds.isEmpty());
      assertEquals(fields.length - lowerBounds.size(), messageIds.size());
      assertEquals(fields.length - messageIds.size(), lowerBounds.size());
   }
}
