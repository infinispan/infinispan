package org.infinispan.objectfilter.impl.aggregation;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class SumAccumulator extends FieldAccumulator {

   private final Class<?> fieldType;

   public SumAccumulator(int pos, Class<?> fieldType) {
      super(pos);
      if (!Number.class.isAssignableFrom(fieldType)) {
         throw new IllegalStateException("Aggregation SUM cannot be applied to property of type " + fieldType.getName());
      }
      this.fieldType = fieldType;
   }

   @Override
   public void update(Object[] srcRow, Object[] destRow) {
      Number value = (Number) srcRow[pos];
      if (value != null) {
         Number sum = (Number) destRow[pos];
         if (sum == null) {
            destRow[pos] = value;
         } else {
            destRow[pos] = add(value, sum);
         }
      }
   }

   private Number add(Number value, Number sum) {
      if (fieldType == Long.class) {
         return sum.longValue() + value.longValue();
      }
      if (fieldType == Float.class) {
         return sum.floatValue() + value.floatValue();
      }
      if (fieldType == Double.class) {
         return sum.doubleValue() + value.doubleValue();
      }
      // byte, short, int
      return sum.intValue() + value.intValue();
   }
}
