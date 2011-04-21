/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
import java.util.Collection;
import java.util.Iterator;

/**
 * Similar to the JDK's {@link java.util.LinkedHashSet} except that it sets the underlying {@link
 * java.util.LinkedHashMap}'s <tt>accessOrder</tt> constructor parameter to <tt>true</tt>, allowing for recording of
 * visits.  To do this, this implementation exposes a {@link #visit(Object)} method to visit a key.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class VisitableBidirectionalLinkedHashSet<E> extends AbstractSet<E> implements ReversibleOrderedSet<E>, Cloneable {

   private transient BidirectionalLinkedHashMap<E, Object> map;

   // Dummy value to associate with an Object in the backing Map
   private static final Object DUMMY_VALUE = new Object();


   /**
    * Constructs a new, empty linked hash set with the specified initial capacity and load factor.
    *
    * @param visitable       if true, visiting an element (using {@link #visit(Object)}) will cause that element to be
    *                        moved to the end of the linked list that connects entries.
    * @param initialCapacity the initial capacity of the linked hash set
    * @param loadFactor      the load factor of the linked hash set
    * @throws IllegalArgumentException if the initial capacity is less than zero, or if the load factor is nonpositive
    */
   public VisitableBidirectionalLinkedHashSet(boolean visitable, int initialCapacity, float loadFactor) {
      map = new BidirectionalLinkedHashMap<E, Object>(initialCapacity, loadFactor, visitable);
   }

   /**
    * Constructs a new, empty linked hash set with the specified initial capacity and the default load factor (0.75).
    *
    * @param visitable       if true, visiting an element (using {@link #visit(Object)}) will cause that element to be
    *                        moved to the end of the linked list that connects entries.
    * @param initialCapacity the initial capacity of the LinkedHashSet
    * @throws IllegalArgumentException if the initial capacity is less than zero
    */
   public VisitableBidirectionalLinkedHashSet(boolean visitable, int initialCapacity) {
      this(visitable, initialCapacity, .75f);
   }

   /**
    * Constructs a new, empty linked hash set with the default initial capacity (16) and load factor (0.75).
    *
    * @param visitable if true, visiting an element (using {@link #visit(Object)}) will cause that element to be moved
    *                  to the end of the linked list that connects entries.
    */
   public VisitableBidirectionalLinkedHashSet(boolean visitable) {
      this(visitable, 16, .75f);
   }

   /**
    * Constructs a new linked hash set with the same elements as the specified collection.  The linked hash set is
    * created with an initial capacity sufficient to hold the elements in the specified collection and the default load
    * factor (0.75).
    *
    * @param visitable if true, visiting an element (using {@link #visit(Object)}) will cause that element to be moved
    *                  to the end of the linked list that connects entries.
    * @param c         the collection whose elements are to be placed into this set
    * @throws NullPointerException if the specified collection is null
    */
   public VisitableBidirectionalLinkedHashSet(boolean visitable, Collection<? extends E> c) {
      this(visitable, Math.max(2 * c.size(), 11), .75f);
      addAll(c);
   }

   /**
    * Returns an iterator over the elements in this set.  The elements are returned in no particular order.
    *
    * @return an Iterator over the elements in this set
    * @see java.util.ConcurrentModificationException
    */
   public Iterator<E> iterator() {
      return map.keySet().iterator();
   }

   public Iterator<E> reverseIterator() {
      return map.keySet().reverseIterator();
   }

   /**
    * Returns the number of elements in this set (its cardinality).
    *
    * @return the number of elements in this set (its cardinality)
    */
   public int size() {
      return map.size();
   }

   /**
    * Returns <tt>true</tt> if this set contains no elements.
    *
    * @return <tt>true</tt> if this set contains no elements
    */
   public boolean isEmpty() {
      return map.isEmpty();
   }

   /**
    * Returns <tt>true</tt> if this set contains the specified element. More formally, returns <tt>true</tt> if and only
    * if this set contains an element <tt>e</tt> such that <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>.
    *
    * @param o element whose presence in this set is to be tested
    * @return <tt>true</tt> if this set contains the specified element
    */
   public boolean contains(Object o) {
      return map.containsKey(o);
   }

   /**
    * Adds the specified element to this set if it is not already present. More formally, adds the specified element
    * <tt>e</tt> to this set if this set contains no element <tt>e2</tt> such that <tt>(e==null&nbsp;?&nbsp;e2==null&nbsp;:&nbsp;e.equals(e2))</tt>.
    * If this set already contains the element, the call leaves the set unchanged and returns <tt>false</tt>.
    *
    * @param e element to be added to this set
    * @return <tt>true</tt> if this set did not already contain the specified element
    */
   public boolean add(E e) {
      return map.put(e, DUMMY_VALUE) == null;
   }

   /**
    * Removes the specified element from this set if it is present. More formally, removes an element <tt>e</tt> such
    * that <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>, if this set contains such an element. Returns
    * <tt>true</tt> if this set contained the element (or equivalently, if this set changed as a result of the call).
    * (This set will not contain the element once the call returns.)
    *
    * @param o object to be removed from this set, if present
    * @return <tt>true</tt> if the set contained the specified element
    */
   public boolean remove(Object o) {
      return map.remove(o) == DUMMY_VALUE;
   }

   /**
    * Visits the key in the underlying Map, by performing a {@link java.util.Map#get(Object)}.  This records the access
    * and updates the ordering accordingly.
    *
    * @param key key to visit
    */
   public void visit(E key) {
      map.get(key);
   }

   /**
    * Removes all of the elements from this set. The set will be empty after this call returns.
    */
   public void clear() {
      map.clear();
   }

   @SuppressWarnings("unchecked")
   public VisitableBidirectionalLinkedHashSet clone() {
      VisitableBidirectionalLinkedHashSet result;
      try {
         result = (VisitableBidirectionalLinkedHashSet) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen", e);
      }

      result.map = map.clone();
      return result;
   }
}
