/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.util;

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
 */
public interface Equivalence<T> extends Serializable {

   /**
    * Returns a hash code value for the object passed.
    *
    * As an example, implementors can provide an alternative implementation
    * for the hash code calculation for arrays. So, instead of relying on
    * {@link Object#hashCode()}, call {@link java.util.Arrays.hashCode()}.
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
    * {@link Object#equals(Object)}}, call {@link java.util.Arrays.equals())}.
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
   int compare(Object obj, Object otherObj); // For future support for objects that are not comparable, i.e. arrays

}
