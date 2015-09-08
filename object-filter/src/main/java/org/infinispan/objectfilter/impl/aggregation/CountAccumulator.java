package org.infinispan.objectfilter.impl.aggregation;

/**
 * COUNT returns a Long greater or equal than 0. Null values are not counted.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class CountAccumulator extends FieldAccumulator {

   public CountAccumulator(int pos) {
      super(pos);
   }

   @Override
   public void init(Object[] row) {
      row[pos] = row[pos] != null ? 1L : 0L;
   }

   @Override
   public void update(Object[] srcRow, Object[] destRow) {
      if (srcRow[pos] != null) {
         destRow[pos] = (Long) destRow[pos] + 1;
      }
   }
}
