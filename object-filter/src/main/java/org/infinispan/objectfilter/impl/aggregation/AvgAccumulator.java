package org.infinispan.objectfilter.impl.aggregation;

/**
 * Computes average of Numbers. The output is always a Double.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class AvgAccumulator extends FieldAccumulator {

   private static class Avg {
      double sum;
      int count;

      Avg(double sum, int count) {
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
   public void init(Object[] row) {
      Number value = (Number) row[pos];
      row[pos] = value != null ? new Avg(value.doubleValue(), 1) : new Avg(0, 0);
   }

   @Override
   public void update(Object[] srcRow, Object[] destRow) {
      Number value = (Number) srcRow[pos];
      if (value != null) {
         Avg avg = (Avg) destRow[pos];
         avg.update(value.doubleValue());
      }
   }

   @Override
   public void finish(Object[] row) {
      row[pos] = ((Avg) row[pos]).getValue();
   }
}
