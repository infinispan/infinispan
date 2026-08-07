package org.infinispan.query.objectfilter.impl.aggregation;

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

   SumAccumulator(int inPos, int outPos, Class<?> fieldType) {
      super(inPos, outPos);
      if (!Number.class.isAssignableFrom(fieldType)) {
         throw new IllegalStateException("Aggregation SUM cannot be applied to property of type " + fieldType.getName());
      }
      this.fieldType = (Class<? extends Number>) fieldType;
   }

   @Override
   public void init(Object[] accRow) {
      if (fieldType == Double.class || fieldType == Float.class) {
         accRow[outPos] = new DoubleStat();
      }
   }

   @Override
   public void update(Object[] accRow, Object val) {
      if (val != null) {
         Number value = (Number) val;
         if (fieldType == Double.class || fieldType == Float.class) {
            ((DoubleStat) accRow[outPos]).update(value.doubleValue());
         } else if (fieldType == Long.class) {
            Number sum = (Number) accRow[outPos];
            accRow[outPos] = sum != null ? sum.longValue() + value.longValue() : value.longValue();
         } else if (fieldType == Integer.class || fieldType == Byte.class || fieldType == Short.class) {
            Number sum = (Number) accRow[outPos];
            accRow[outPos] = sum != null ? sum.intValue() + value.intValue() : value.intValue();
         } else if (fieldType == BigInteger.class) {
            BigInteger sum = (BigInteger) accRow[outPos];
            accRow[outPos] = sum != null ? sum.add((BigInteger) value) : value;
         } else if (fieldType == BigDecimal.class) {
            BigDecimal sum = (BigDecimal) accRow[outPos];
            accRow[outPos] = sum != null ? sum.add((BigDecimal) value) : value;
         }
      }
   }

   @Override
   protected void merge(Object[] accRow, Object value) {
      if (value instanceof DoubleStat) {
         value = ((DoubleStat) value).getSum();
      } else if (value instanceof Counter) {
         value = ((Counter) value).getValue();
      }
      update(accRow, value);
   }

   @Override
   protected void finish(Object[] accRow) {
      if (fieldType == Double.class || fieldType == Float.class) {
         accRow[outPos] = ((DoubleStat) accRow[outPos]).getSum();
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
      return fieldType;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || o.getClass() != getClass()) return false;
      SumAccumulator other = (SumAccumulator) o;
      return inPos == other.inPos && outPos == other.outPos && fieldType == other.fieldType;
   }

   @Override
   public int hashCode() {
      return 31 * super.hashCode() + fieldType.hashCode();
   }
}
