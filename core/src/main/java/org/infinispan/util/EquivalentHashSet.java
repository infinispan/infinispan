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

import java.util.AbstractSet;
import java.util.Iterator;

/**
 * Custom hash-based set which accepts no null values, where
 * equality and hash code calculations are done based on passed
 * {@link org.infinispan.util.Equivalence} function implementations for values,
 * as opposed to relying on their own equals/hashCode/toString implementations.
 * This is handy when using key/values whose mentioned methods cannot be
 * overriden, i.e. arrays, and in situations where users want to avoid using
 * wrapper objects.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class EquivalentHashSet<E> extends AbstractSet<E> {

   /**
    * Equivalence function for the entries of this hash set, providing
    * functionality to check equality, calculate hash codes...etc of the
    * stored entries.
    */
   private final Equivalence<E> entryEq;

   /**
    * The underlying map. Uses Boolean.TRUE as value for each element.
    */
   private final EquivalentHashMap<E, Boolean> m;

   /**
    * Constructs a new, empty set, with a given equivalence function
    *
    * @param entryEq the Equivalence function to be used to compare entries
    *                in this set.
    */
   public EquivalentHashSet(Equivalence<E> entryEq) {
      this.entryEq = entryEq;
      m = new EquivalentHashMap<E, Boolean>(entryEq, AnyEquivalence.BOOLEAN);
   }

   /**
    * Constructs a new, empty set, with a given initial capacity and a
    * particular equivalence function to compare entries.
    *
    * @param initialCapacity this set's initial capacity
    * @param entryEq the Equivalence function to be used to compare entries
    *                in this set.
    */
   public EquivalentHashSet(int initialCapacity, Equivalence<E> entryEq) {
      this.entryEq = entryEq;
      m = new EquivalentHashMap<E, Boolean>(
            initialCapacity, entryEq, AnyEquivalence.BOOLEAN);
   }

   /**
    * Returns an iterator over the elements in this set.
    *
    * @return an iterator over the elements in this set
    */
   @Override
   public Iterator<E> iterator() {
      return m.keySet().iterator();
   }

   /**
    * Returns the number of elements in this set.  If this set
    * contains more than {@code Integer.MAX_VALUE} elements, it
    * returns {@code Integer.MAX_VALUE}.
    *
    * @return  the number of elements in this set
    */
   @Override
   public int size() {
      return m.size();
   }

   /**
    * Returns {@code true} if this set contains no elements.
    *
    * @return {@code true} if this set contains no elements
    */
   @Override
   public boolean isEmpty() {
      return m.isEmpty();
   }

   /**
    * Returns {@code true} if this set contains the specified element.
    *
    * @param o the object to be checked for containment in this set
    * @return {@code true} if this set contains the specified element
    */
   @Override
   public boolean contains(Object o) {
      return m.containsKey(o);
   }

   /**
    * Adds the specified element to this set if it is not already present.
    *
    * @param o element to be added to this set
    * @return {@code true} if the set did not already contain the specified
    *         element
    */
   @Override
   public boolean add(E o) {
      return m.put(o, Boolean.TRUE) == null;
   }

   /**
    * Removes the specified element from this set if it is present.
    *
    * @param o object to be removed from this set, if present
    * @return {@code true} if the set contained the specified element
    */
   @Override
   public boolean remove(Object o) {
      return m.remove(o) != null;
   }

   /**
    * Removes all of the elements from this set.
    */
   @Override
   public void clear() {
      m.clear();
   }

   /**
    * Returns the hash code value for this set using the {@link Equivalence}
    * function associated with it.  The hash code of a set is defined to be
    * the sum of the hash codes of the elements in the set, where the hash
    * code of a <tt>null</tt> element is defined to be zero. This ensures that
    * {@link Equivalence#equals(Object s1, Object s2)} implies that
    * {@link Equivalence#hashCode(Object s1)}=={@link Equivalence#hashCode(Object s2)}
    * for any two sets <tt>s1</tt> and <tt>s2</tt>, as required by the general
    * contract of {@link Object#hashCode}.
    *
    * <p>This implementation iterates over the set, calling the
    * <tt>hashCode</tt> method on each element in the set, and adding up
    * the results.
    *
    * @return the hash code value for this set
    */
   @Override
   public int hashCode() {
      int h = 0;
      Iterator<E> i = iterator();
      while (i.hasNext()) {
         E obj = i.next();
         if (obj != null)
            h += entryEq.hashCode(obj);
      }
      return h;
   }

}
