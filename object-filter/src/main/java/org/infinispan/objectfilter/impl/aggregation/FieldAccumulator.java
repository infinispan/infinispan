package org.infinispan.objectfilter.impl.aggregation;

/**
 * An accumulator is a stateless object that operates on row data.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public abstract class FieldAccumulator {

   /**
    * Column.
    */
   protected final int pos;

   protected FieldAccumulator(int pos) {
      this.pos = pos;
   }

   public void init(Object[] accRow) {
   }

   public abstract void update(Object[] srcRow, Object[] accRow);

   public void finish(Object[] accRow) {
   }
}
