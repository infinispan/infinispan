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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.util.Collections.reverse;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

/**
 * Static helpers for Infinispan-specific collections
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class InfinispanCollections {
   private static final ReversibleOrderedSet EMPTY_ROS = new EmptyReversibleOrderedSet();
   public static final BidirectionalMap EMPTY_BIDI_MAP = new EmptyBidiMap();

   @SuppressWarnings("unchecked")
   public static <T> ReversibleOrderedSet<T> emptyReversibleOrderedSet() {
      return EMPTY_ROS;
   }

   @SuppressWarnings("unchecked")
   public static <K, V> BidirectionalMap<K, V> emptyBidirectionalMap() {
      return EMPTY_BIDI_MAP;
   }

   private static final class EmptyReversibleOrderedSet extends AbstractSet implements ReversibleOrderedSet {

      Iterator it = new Iterator() {

         public boolean hasNext() {
            return false;
         }

         public Object next() {
            throw new NoSuchElementException();
         }

         public void remove() {
            throw new UnsupportedOperationException();
         }
      };

      public Iterator iterator() {
         return it;
      }

      public int size() {
         return 0;
      }

      public Iterator reverseIterator() {
         return it;
      }
   }

   private static final class EmptyBidiMap extends AbstractMap implements BidirectionalMap {

      public int size() {
         return 0;
      }

      public boolean isEmpty() {
         return true;
      }

      public boolean containsKey(Object key) {
         return false;
      }

      public boolean containsValue(Object value) {
         return false;
      }

      public Object get(Object key) {
         return null;
      }

      public Object put(Object key, Object value) {
         throw new UnsupportedOperationException();
      }

      public Object remove(Object key) {
         throw new UnsupportedOperationException();
      }

      public void putAll(Map t) {
         throw new UnsupportedOperationException();
      }

      public void clear() {
         throw new UnsupportedOperationException();
      }

      public ReversibleOrderedSet keySet() {
         return EMPTY_ROS;
      }

      public Collection values() {
         return Collections.emptySet();
      }

      public ReversibleOrderedSet entrySet() {
         return EMPTY_ROS;
      }
   }

   // Below you can find some functional programming style helpers

   /**
    * A function that converts a type into another one.
    *
    * @param <E> Input type.
    * @param <T> Output type.
    */
   public static interface Function<E, T> {

      /**
       * Transforms an instance of the given input into an instace of the
       * type to be returned.
       *
       * @param input Instance of the input type.
       * @return Instance of the output type.
       */
      T transform(E input);
   }

   /**
    * Given a map of well known key/value types, it makes a swallow copy of it
    * while at the same time transforming it's value type to a desired output
    * type. The transformation of the value type is done using a given a
    * function.
    *
    * @param input contains the input key/value pair map
    * @param f function instance to use to transform the value part of the map
    * @param <K> input map's key type
    * @param <V> desired output type of the map's value
    * @param <E> input map's value type
    * @return a swallow copy of the input Map with all its values transformed.
    */
   public static <K, V, E> Map<K, V> transformMapValue(Map<K, E> input, Function<E, V> f) {
      // This screams for a map function! Gimme functional programming pleasee...
      if (input.size() == 1) {
         Map.Entry<K, E> single = input.entrySet().iterator().next();
         return singletonMap(single.getKey(), f.transform(single.getValue()));
      } else {
         Map<K, V> copy = new HashMap<K, V>(input.size());
         for (Map.Entry<K, E> entry : input.entrySet())
            copy.put(entry.getKey(), f.transform(entry.getValue()));
         return unmodifiableMap(copy);
      }
   }

}
