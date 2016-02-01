package org.infinispan.objectfilter.impl.aggregation;

import org.infinispan.objectfilter.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * Computes the average of {@link Number}s. The output type is always a {@link Double}. Nulls are excluded from the
 * computation. If there are no remaining non-null values to which the aggregate function can be applied, the result of
 * the aggregate function is {@code null}.
 * <p>
 * The implementation uses compensated summation in order to reduce the error bound in the numerical sum compared to a
 * simple summation of {@code double} values similar to the way {@link java.util.DoubleSummaryStatistics} works.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
final class AvgAccumulator extends FieldAccumulator {

   private static final Log log = Logger.getMessageLogger(Log.class, AvgAccumulator.class.getName());

   AvgAccumulator(int inPos, int outPos, Class<?> fieldType) {
      super(inPos, outPos);
      if (!Number.class.isAssignableFrom(fieldType)) {
         throw log.getAVGCannotBeAppliedToPropertyOfType(fieldType.getName());
      }
   }

   @Override
   public void init(Object[] accRow) {
      accRow[outPos] = new DoubleStat();
   }

   @Override
   public void update(Object[] accRow, Object value) {
      if (value != null) {
         ((DoubleStat) accRow[outPos]).update(((Number) value).doubleValue());
      }
   }

   @Override
   protected void merge(Object[] accRow, Object value) {
      if (value instanceof DoubleStat) {
         DoubleStat avgVal = (DoubleStat) value;
         if (avgVal.getCount() > 0) {
            ((DoubleStat) accRow[outPos]).update(avgVal.getSum(), avgVal.getCount());
         }
      } else {
         update(accRow, value);
      }
   }

   @Override
   protected void finish(Object[] accRow) {
      accRow[outPos] = ((DoubleStat) accRow[outPos]).getAvg();
   }
}
