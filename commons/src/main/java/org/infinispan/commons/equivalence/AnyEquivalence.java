package org.infinispan.commons.equivalence;

/**
 * A compare function for objects.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public final class AnyEquivalence<T> implements Equivalence<T> {

   private static AnyEquivalence<Object> OBJECT = new AnyEquivalence<Object>();

   public static AnyEquivalence<String> STRING = getInstance(String.class);

   public static AnyEquivalence<Byte> BYTE = getInstance(Byte.class);

   public static AnyEquivalence<Short> SHORT = getInstance(Short.class);

   public static AnyEquivalence<Integer> INT = getInstance(Integer.class);

   public static AnyEquivalence<Long> LONG = getInstance(Long.class);

   public static AnyEquivalence<Double> DOUBLE  = getInstance(Double.class);

   public static AnyEquivalence<Float> FLOAT = getInstance(Float.class);

   public static AnyEquivalence<Boolean> BOOLEAN = getInstance(Boolean.class);

   // To avoid instantiation
   private AnyEquivalence() {
   }

   @Override
   public int hashCode(Object obj) {
      return obj.hashCode();
   }

   @Override
   public boolean equals(T obj, Object otherObj) {
      return obj != null && obj.equals(otherObj);
   }

   @Override
   public String toString(Object obj) {
      return obj.toString();
   }

   @Override
   public boolean isComparable(Object obj) {
      return obj instanceof Comparable;
   }

   @Override
   @SuppressWarnings("unchecked")
   public int compare(T obj, T otherObj) {
      return ((Comparable<T>) obj).compareTo(otherObj);
   }

   @SuppressWarnings("unchecked")
   public static <T> AnyEquivalence<T> getInstance() {
      return (AnyEquivalence<T>) OBJECT;
   }

   @SuppressWarnings("unchecked")
   public static <T> AnyEquivalence<T> getInstance(Class<T> classType) {
      return (AnyEquivalence<T>) OBJECT;
   }
}
