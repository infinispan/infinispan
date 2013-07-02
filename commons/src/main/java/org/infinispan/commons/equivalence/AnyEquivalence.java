package org.infinispan.commons.equivalence;

/**
 * A compare function for objects.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public final class AnyEquivalence<T> implements Equivalence<T> {

   private static AnyEquivalence<Object> OBJECT = new AnyEquivalence<Object>();

   public static AnyEquivalence<String> STRING = new AnyEquivalence<String>();

   public static AnyEquivalence<Byte> BYTE = new AnyEquivalence<Byte>();

   public static AnyEquivalence<Short> SHORT = new AnyEquivalence<Short>();

   public static AnyEquivalence<Integer> INT = new AnyEquivalence<Integer>();

   public static AnyEquivalence<Long> LONG = new AnyEquivalence<Long>();

   public static AnyEquivalence<Double> DOUBLE  = new AnyEquivalence<Double>();

   public static AnyEquivalence<Float> FLOAT = new AnyEquivalence<Float>();

   public static AnyEquivalence<Boolean> BOOLEAN = new AnyEquivalence<Boolean>();

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

}
