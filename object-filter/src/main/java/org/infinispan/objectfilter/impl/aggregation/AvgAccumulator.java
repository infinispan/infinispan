package org.infinispan.objectfilter.impl.aggregation;

/**
 * Computes the average of {@link Number}s. The output type is always a {@link Double}. Nulls are excluded from the
 * computation. If there are no remaining non-null values to which the aggregate function can be applied, the result of
 * the aggregate function is {@code null}.
 * <p/>
 * The implementation uses compensated summation in order to reduce the error bound in the numerical sum compared to a
 * simple summation of {@code double} values similar to the way {@link java.util.DoubleSummaryStatistics} works.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class AvgAccumulator extends FieldAccumulator {

   public AvgAccumulator(int pos, Class<?> fieldType) {
      super(pos);
      if (!Number.class.isAssignableFrom(fieldType)) {
         throw new IllegalStateException("Aggregation AVG cannot be applied to property of type " + fieldType.getName());
      }
   }

   @Override
   public void init(Object[] accRow) {
      Number value = (Number) accRow[pos];
      DoubleAvg doubleAvg = new DoubleAvg();
      accRow[pos] = doubleAvg;
      if (value != null) {
         doubleAvg.update(value.doubleValue());
      }
   }

   @Override
   public void update(Object[] srcRow, Object[] accRow) {
      Number value = (Number) srcRow[pos];
      if (value != null) {
         DoubleAvg doubleAvg = (DoubleAvg) accRow[pos];
         doubleAvg.update(value.doubleValue());
      }
   }

   @Override
   public void finish(Object[] accRow) {
      accRow[pos] = ((DoubleAvg) accRow[pos]).getValue();
   }
}
