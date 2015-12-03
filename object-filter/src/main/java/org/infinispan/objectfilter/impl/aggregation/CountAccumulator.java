package org.infinispan.objectfilter.impl.aggregation;

/**
 * Counts the encountered values and returns a {@code Long} greater or equal than 0. Null values are not counted. If
 * there are no non-null values to which COUNT can be applied, the result of the aggregate function is 0.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
final class CountAccumulator extends FieldAccumulator {

   public CountAccumulator(int inPos, int outPos) {
      super(inPos, outPos);
   }

   @Override
   public void init(Object[] accRow) {
      accRow[outPos] = 0L;
   }

   @Override
   public void update(Object[] accRow, Object value) {
      if (value != null) {
         accRow[outPos] = (Long) accRow[outPos] + 1;
      }
   }
}
