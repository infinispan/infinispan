package org.infinispan.objectfilter.impl.aggregation;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class MaxAccumulator extends FieldAccumulator {

   public MaxAccumulator(int pos, Class<?> fieldType) {
      super(pos);
      if (!Comparable.class.isAssignableFrom(fieldType)) {
         throw new IllegalStateException("Aggregation MAX cannot be applied to property of type " + fieldType.getName());
      }
   }

   @Override
   public void update(Object[] srcRow, Object[] destRow) {
      Comparable value = (Comparable) srcRow[pos];
      if (value != null) {
         Comparable max = (Comparable) destRow[pos];
         if (max == null || max.compareTo(value) < 0) {
            destRow[pos] = value;
         }
      }
   }
}
