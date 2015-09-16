package org.infinispan.objectfilter.impl.aggregation;

/**
 * Computes the average of {@link Number}s. The output type is always a {@link Double}. Nulls are excluded from the
 * computation. If there are no remaining non-null values to which the aggregate function can be applied, the result of
 * the aggregate function is {@code null}.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class AvgAccumulator extends FieldAccumulator {

   private static class Avg {
      double sum;
      long count;

      Avg(double sum, long count) {
         this.sum = sum;
         this.count = count;
      }

      void update(double value) {
         sum += value;
         count++;
      }

      Double getValue() {
         return count > 0 ? sum / count : null;
      }
   }

   public AvgAccumulator(int pos, Class<?> fieldType) {
      super(pos);
      if (!Number.class.isAssignableFrom(fieldType)) {
         throw new IllegalStateException("Aggregation AVG cannot be applied to property of type " + fieldType.getName());
      }
   }

   @Override
   public void init(Object[] accRow) {
      Number value = (Number) accRow[pos];
      accRow[pos] = value != null ? new Avg(value.doubleValue(), 1) : new Avg(0, 0);
   }

   @Override
   public void update(Object[] srcRow, Object[] accRow) {
      Number value = (Number) srcRow[pos];
      if (value != null) {
         Avg avg = (Avg) accRow[pos];
         avg.update(value.doubleValue());
      }
   }

   @Override
   public void finish(Object[] accRow) {
      accRow[pos] = ((Avg) accRow[pos]).getValue();
   }
}
