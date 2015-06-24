package org.infinispan.objectfilter.impl.aggregation;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class MinAccumulator extends FieldAccumulator {

   public MinAccumulator(int pos, Class<?> fieldType) {
      super(pos);
      if (!Comparable.class.isAssignableFrom(fieldType)) {
         throw new IllegalStateException("Aggregation MIN cannot be applied to property of type " + fieldType.getName());
      }
   }

   @Override
   public void update(Object[] srcRow, Object[] destRow) {
      Comparable value = (Comparable) srcRow[pos];
      if (value != null) {
         Comparable min = (Comparable) destRow[pos];
         if (min == null || min.compareTo(value) > 0) {
            destRow[pos] = value;
         }
      }
   }
}
