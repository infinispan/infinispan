package org.infinispan.commons.equivalence;

import java.io.Serializable;

/**
 * An interface that allows custom implementations for objects that are
 * comparable based on equality, hash code calculations, or according to
 * {@link Comparable} rules, but where the behaviour provided by the JDK, or
 * external libraries, cannot be modified, for example: arrays.
 *
 * The interface is marked to be {@link Serializable} because multiple
 * collection extensions within the Infinispan code base contain references
 * to them, and since these are potentially {@link Serializable}, they might
 * be persisted somehow.
 *
 * @author Galder Zamarreño
 * @since 5.3
 * @Deprecated Since 9.0, Equivalence is to be removed (byte[] are directly supported)
 */
@Deprecated
public interface Equivalence<T> extends Serializable {

   /**
    * Returns a hash code value for the object passed.
    *
    * As an example, implementors can provide an alternative implementation
    * for the hash code calculation for arrays. So, instead of relying on
    * {@link Object#hashCode()}, call {@link java.util.Arrays#hashCode()}.
    *
    * @param obj instance to calculate hash code for
    * @return a hash code value for the object passed as parameter
    */
   int hashCode(Object obj);

   /**
    * Indicates whether the objects passed are "equal to" each other.
    *
    * As an example, implementors can provide an alternative implementation
    * for the equals for arrays. So, instead of relying on
    * {@link Object#equals(Object)}}, call {@link java.util.Arrays#equals(Object[], Object[])}.
    *
    * @param obj to be compared with second parameter
    * @param otherObj to be compared with first parameter
    * @return <code>true</code> if both objects are the same;
    *         <code>false</code> otherwise
    */
   boolean equals(T obj, Object otherObj);

   /**
    * Returns a string representation of the given object.
    *
    * @param obj whose string representation is to be returned
    * @return a string representation of the passed object
    */
   String toString(Object obj);

   /**
    * Returns whether the given object is comparable. In other words, if
    * given an instance of the object, a sensible comparison can be computed
    * using {@link #compare(Object, Object)} method.
    *
    * @param obj instance to check if it's comparable
    * @return <code>true</code> if the object is comparable;
    *         <code>false</code> otherwise
    */
   boolean isComparable(Object obj); // For future support for objects that are not comparable, i.e. arrays

   /**
    * Compares the two given objects for order. Returns a negative integer,
    * zero, or a positive integer as the first object is less than, equal to,
    * or greater than the second object.
    *
    * @param obj first object to be compared
    * @param otherObj second object to be compared
    * @return a negative integer, zero, or a positive integer as the
    *         first object is less than, equal to, or greater than the
    *         second object
    */
   int compare(T obj, T otherObj); // For future support for objects that are not comparable, i.e. arrays

}
