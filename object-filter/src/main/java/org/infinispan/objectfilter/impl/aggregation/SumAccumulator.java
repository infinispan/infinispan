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
public final class SumAccumulator extends FieldAccumulator {

   private final Class<? extends Number> fieldType;

   public SumAccumulator(int pos, Class<?> fieldType) {
      super(pos);
      if (!Number.class.isAssignableFrom(fieldType)) {
         throw new IllegalStateException("Aggregation SUM cannot be applied to property of type " + fieldType.getName());
      }
      this.fieldType = (Class<? extends Number>) fieldType;
   }

   @Override
   public void init(Object[] accRow) {
      Number value = (Number) accRow[pos];
      if (value != null) {
         accRow[pos] = widen(value);
      }
   }

   @Override
   public void update(Object[] srcRow, Object[] accRow) {
      Number value = (Number) srcRow[pos];
      if (value != null) {
         value = widen(value);
         Number sum = (Number) accRow[pos];
         if (sum == null) {
            accRow[pos] = value;
         } else {
            accRow[pos] = add(value, sum);
         }
      }
   }

   private Number widen(Number value) {
      if (fieldType == Integer.class || fieldType == Byte.class || fieldType == Short.class) {
         return value.longValue();
      } else if (fieldType == Float.class) {
         return value.doubleValue();
      }
      return value;
   }

   private Number add(Number value, Number sum) {
      if (fieldType == Long.class) {
         return sum.longValue() + value.longValue();
      }
      if (fieldType == Float.class) {
         return sum.doubleValue() + value.doubleValue();
      }
      if (fieldType == Double.class) {
         return sum.doubleValue() + value.doubleValue();
      }
      if (fieldType == BigInteger.class) {
         return ((BigInteger) sum).add((BigInteger) value);
      }
      if (fieldType == BigDecimal.class) {
         return ((BigDecimal) sum).add((BigDecimal) value);
      }
      // byte, short, int
      return sum.intValue() + value.intValue();
   }
}
