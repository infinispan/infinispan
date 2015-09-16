package org.infinispan.objectfilter.impl.aggregation;

/**
 * Counts the encountered values and returns a {@code Long} greater or equal than 0. Null values are not counted. If there are
 * no non-null values to which COUNT can be applied, the result of the aggregate function is 0.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class CountAccumulator extends FieldAccumulator {

   public CountAccumulator(int pos) {
      super(pos);
   }

   @Override
   public void init(Object[] accRow) {
      accRow[pos] = accRow[pos] != null ? 1L : 0L;
   }

   @Override
   public void update(Object[] srcRow, Object[] accRow) {
      if (srcRow[pos] != null) {
         accRow[pos] = (Long) accRow[pos] + 1;
      }
   }
}
