package org.infinispan.objectfilter.impl.aggregation;

import org.infinispan.objectfilter.PropertyPath;

/**
 * An accumulator is a stateless object that operates on row data.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public abstract class FieldAccumulator {

   /**
    * Input column.
    */
   protected final int inPos;

   /**
    * Output column.
    */
   protected final int outPos;

   protected FieldAccumulator(int inPos, int outPos) {
      this.inPos = inPos;
      this.outPos = outPos;
   }

   public void init(Object[] accRow) {
   }

   public void update(Object[] srcRow, Object[] accRow) {
      update(accRow, srcRow[inPos]);
   }

   public abstract void update(Object[] accRow, Object value);

   public void finish(Object[] accRow) {
   }

   public static FieldAccumulator makeAccumulator(PropertyPath.AggregationType aggregationType, int inColumn, int outColumn, Class<?> propertyType) {
      switch (aggregationType) {
         case SUM:
            return new SumAccumulator(inColumn, outColumn, propertyType);
         case AVG:
            return new AvgAccumulator(inColumn, outColumn, propertyType);
         case MIN:
            return new MinAccumulator(inColumn, outColumn, propertyType);
         case MAX:
            return new MaxAccumulator(inColumn, outColumn, propertyType);
         case COUNT:
            return new CountAccumulator(inColumn, outColumn);
         default:
            throw new IllegalArgumentException("Aggregation " + aggregationType.name() + " is not supported");
      }
   }

   public static Class<?> getOutputType(PropertyPath.AggregationType aggregationType, Class<?> propertyType) {
      if (aggregationType == PropertyPath.AggregationType.AVG) {
         return Double.class;
      } else if (aggregationType == PropertyPath.AggregationType.COUNT) {
         return Long.class;
      } else if (aggregationType == PropertyPath.AggregationType.SUM) {
         return SumAccumulator.getOutputType(propertyType);
      }
      return propertyType;
   }
}
