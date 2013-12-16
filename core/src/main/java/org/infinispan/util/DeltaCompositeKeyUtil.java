package org.infinispan.util;

import org.infinispan.atomic.DeltaCompositeKey;
import org.infinispan.context.impl.TxInvocationContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utility methods to deal with the DeltaCompositeKey.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class DeltaCompositeKeyUtil {

   private DeltaCompositeKeyUtil() { /* no-op */ }

   public static Collection<Object> getAffectedKeysFromContext(TxInvocationContext context) {
      return filterDeltaCompositeKeys(context.getAffectedKeys());
   }

   public static Collection<Object> filterDeltaCompositeKeys(Collection<Object> keys) {
      if (keys == null || keys.isEmpty()) {
         return keys;
      }
      List<Object> list = new ArrayList<Object>(keys.size());
      for (Object key : keys) {
         list.add(filterDeltaCompositeKey(key));
      }
      return list;
   }

   public static Object filterDeltaCompositeKey(Object key) {
      if (key == null || !(key instanceof DeltaCompositeKey)) {
         return key;
      }
      return ((DeltaCompositeKey) key).getDeltaAwareValueKey();
   }

}
