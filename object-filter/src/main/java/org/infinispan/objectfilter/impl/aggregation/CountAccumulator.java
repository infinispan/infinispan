package org.infinispan.objectfilter.impl.aggregation;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class CountAccumulator extends FieldAccumulator {

   public CountAccumulator(int pos) {
      super(pos);
   }

   @Override
   public void init(Object[] row) {
      row[pos] = 1;
   }

   @Override
   public void update(Object[] srcRow, Object[] destRow) {
      destRow[pos] = (Integer) destRow[pos] + 1;
   }
}
