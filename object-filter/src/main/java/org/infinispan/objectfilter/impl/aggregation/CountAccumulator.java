package org.infinispan.objectfilter.impl.aggregation;

/**
 * Counts the encountered values and returns a {@code Long} greater or equal than 0. Null values are not counted. If
 * there are no non-null values to which COUNT can be applied, the result of the aggregate function is 0.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
final class CountAccumulator extends FieldAccumulator {

   protected CountAccumulator(int inPos, int outPos) {
      super(inPos, outPos);
   }

   @Override
   public void init(Object[] accRow) {
      accRow[outPos] = new Counter();
   }

   @Override
   protected void merge(Object[] accRow, Object value) {
      if (value instanceof Counter) {
         ((Counter) accRow[outPos]).add(((Counter) value).getValue());
      } else if (value != null) {
         ((Counter) accRow[outPos]).add(1);
      }
   }

   @Override
   public void update(Object[] accRow, Object value) {
      if (value != null) {
         ((Counter) accRow[outPos]).inc();
      }
   }

   @Override
   protected void finish(Object[] accRow) {
      accRow[outPos] = ((Counter) accRow[outPos]).getValue();
   }
}
