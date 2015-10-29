package org.infinispan.objectfilter.impl.aggregation;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Computes the sum of {@link Number}s. Returns {@link Long} when applied to fields of integral types (other than
 * BigInteger); Double when applied to state-fields of floating point types; BigInteger when applied to state-fields of
 * type BigInteger; and BigDecimal when applied to state-fields of type BigDecimal. Nulls are excluded from the
 * computation. If there are no remaining non-null values to which the aggregate function can be applied, the result of
 * the aggregate function is {@code null}.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
final class SumAccumulator extends FieldAccumulator {

   private final Class<? extends Number> fieldType;

   public SumAccumulator(int inPos, int outPos, Class<?> fieldType) {
      super(inPos, outPos);
      if (!Number.class.isAssignableFrom(fieldType)) {
         throw new IllegalStateException("Aggregation SUM cannot be applied to property of type " + fieldType.getName());
      }
      this.fieldType = (Class<? extends Number>) fieldType;
   }

   @Override
   public void init(Object[] accRow) {
      if (fieldType == Double.class || fieldType == Float.class) {
         accRow[outPos] = new DoubleSum();
      }
   }

   @Override
   public void update(Object[] srcRow, Object[] accRow) {
      Number value = (Number) srcRow[inPos];
      if (value != null) {
         if (fieldType == Double.class || fieldType == Float.class) {
            ((DoubleSum) accRow[outPos]).update(value.doubleValue());
         } else if (fieldType == Integer.class || fieldType == Byte.class || fieldType == Short.class) {
            value = value.longValue();
            Number sum = (Number) accRow[outPos];
            if (sum != null) {
               if (fieldType == Long.class) {
                  value = sum.longValue() + value.longValue();
               } else if (fieldType == BigInteger.class) {
                  value = ((BigInteger) sum).add((BigInteger) value);
               } else if (fieldType == BigDecimal.class) {
                  value = ((BigDecimal) sum).add((BigDecimal) value);
               } else {
                  // byte, short, int
                  value = sum.intValue() + value.intValue();
               }
            }
            accRow[outPos] = value;
         }
      }
   }

   @Override
   public void finish(Object[] accRow) {
      if (fieldType == Double.class || fieldType == Float.class) {
         accRow[outPos] = ((DoubleSum) accRow[outPos]).getValue();
      }
   }

   /**
    * Determine the output type of this accumulator.
    */
   static Class<?> getOutputType(Class<?> fieldType) {
      if (!Number.class.isAssignableFrom(fieldType)) {
         throw new IllegalStateException("Aggregation SUM cannot be applied to property of type " + fieldType.getName());
      }
      if (fieldType == Double.class || fieldType == Float.class) {
         return Double.class;
      }
      if (fieldType == Long.class || fieldType == Integer.class || fieldType == Byte.class || fieldType == Short.class) {
         return Long.class;
      }
      return fieldType;
   }
}
