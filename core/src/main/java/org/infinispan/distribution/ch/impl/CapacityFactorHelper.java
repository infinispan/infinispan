package org.infinispan.distribution.ch.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Provides equality and hashing for capacity factors that treats {@code null} as semantically equivalent to all-1.0f
 * (every member at default capacity).
 *
 * <p>
 * Capacity factors may be stored as {@code null} when not explicitly configured, or as a collection of {@code 1.0f} values
 * when set to defaults. Both representations carry the same meaning: every member owns its proportional share of segments.
 * This helper ensures consistent behavior in {@code equals()} and {@code hashCode()} across consistent hash implementations.
 * </p>
 */
final class CapacityFactorHelper {

   private CapacityFactorHelper() { }

   public static int capacityFactorHashCode(List<Float> cf) {
      if (isDefaultCapacityFactor(cf)) return 0;
      return Objects.hashCode(cf);
   }

   public static int capacityFactorHashCode(Map<?, Float> cf) {
      if (isDefaultCapacityFactor(cf)) return 0;
      return Objects.hashCode(cf);
   }

   public static boolean isCapacityFactorsEquals(List<Float> a, List<Float> b) {
      if (a == null)
         return isDefaultCapacityFactor(b);

      if (b == null)
         return isDefaultCapacityFactor(a);

      return Objects.equals(a, b);
   }

   public static boolean isCapacityFactorsEquals(Map<?, Float> a, Map<?, Float> b) {
      if (a == null)
         return isDefaultCapacityFactor(b);

      if (b == null)
         return isDefaultCapacityFactor(a);

      return Objects.equals(a, b);
   }

   public static boolean isDefaultCapacityFactor(Map<?, Float> cf) {
      if (cf == null)
         return true;

      return isDefaultCapacityFactor(cf.values());
   }

   public static boolean isDefaultCapacityFactor(Collection<Float> cf) {
      // A null capacity factor is considered default.
      // It is semantically equivalent of an array of 1 capacity to all nodes.
      if (cf == null)
         return true;

      // If not null, then all values should be 1 to be considered default.
      for (Float v : cf) {
         if (Float.compare(v, 1.0f) != 0)
            return false;
      }

      return true;
   }
}
