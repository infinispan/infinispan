package org.infinispan.query.objectfilter.impl.syntax.update;

import java.util.Locale;

/**
 * Range checks for integral values being narrowed to a smaller integer type.
 * <p>
 * Shared by the parse-time type checker and the runtime value conversion, so the
 * bound of every integer type is defined in exactly one place.
 *
 * @since 16.4
 */
public final class IntegerRange {

   private IntegerRange() {
   }

   /**
    * Whether the integral {@code value} fits the integer type denoted by {@code typeName}:
    * {@code int32}, {@code int64} or the simple name of a Java integer type
    * (e.g. {@code int}, {@code Integer}, {@code Short}, {@code byte}).
    * <p>
    * Non-integral and non-finite values never fit.
    */
   public static boolean fits(Number value, String typeName) {
      return fitsBounds(value, boundsOf(typeName.toLowerCase(Locale.ROOT)));
   }

   /**
    * Whether the integral {@code value} fits the given Java integer type.
    * <p>
    * Non-integral and non-finite values never fit.
    */
   public static boolean fits(Number value, Class<?> typeName) {
      return fitsBounds(value, boundsOf(typeName));
   }

   private static boolean fitsBounds(Number value, Bounds bounds) {
      double dv = value.doubleValue();
      if (Double.isNaN(dv) || Double.isInfinite(dv) || dv != Math.floor(dv)) {
         return false;
      }
      if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
         long lv = value.longValue();
         return lv >= bounds.min() && lv <= bounds.max();
      }
      // 2^63 is the first double above Long.MAX_VALUE; (double) Long.MAX_VALUE rounds up to it.
      return dv >= (double) bounds.min()
            && (bounds.max() == Long.MAX_VALUE ? dv < 0x1.0p63 : dv <= (double) bounds.max());
   }

   private static Bounds boundsOf(String type) {
      return switch (type) {
         case "byte" -> new Bounds(Byte.MIN_VALUE, Byte.MAX_VALUE);
         case "short" -> new Bounds(Short.MIN_VALUE, Short.MAX_VALUE);
         case "int", "integer", "int32" -> new Bounds(Integer.MIN_VALUE, Integer.MAX_VALUE);
         default -> new Bounds(Long.MIN_VALUE, Long.MAX_VALUE);
      };
   }

   private static Bounds boundsOf(Class<?> type) {
      if (type == byte.class || type == Byte.class) {
         return new Bounds(Byte.MIN_VALUE, Byte.MAX_VALUE);
      }
      if (type == short.class || type == Short.class) {
         return new Bounds(Short.MIN_VALUE, Short.MAX_VALUE);
      }
      if (type == int.class || type == Integer.class) {
         return new Bounds(Integer.MIN_VALUE, Integer.MAX_VALUE);
      }
      return new Bounds(Long.MIN_VALUE, Long.MAX_VALUE);
   }

   private record Bounds(long min, long max) {
   }
}
