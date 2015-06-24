package org.infinispan.objectfilter.impl.aggregation;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public abstract class FieldAccumulator {

   protected final int pos;

   protected FieldAccumulator(int pos) {
      this.pos = pos;
   }

   public void init(Object[] row) {
   }

   public abstract void update(Object[] srcRow, Object[] destRow);

   public void finish(Object[] row) {
   }
}
