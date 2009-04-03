package org.horizon.util;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Static helpers for Horizon-specific collections
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class HorizonCollections {
   private static final ReversibleOrderedSet EMPTY_ROS = new EmptyReversibleOrderedSet();
   private static final BidirectionalMap EMPTY_BIDI_MAP = new EmptyBidiMap();

   @SuppressWarnings("unchecked")
   public static final <T> ReversibleOrderedSet<T> emptyReversibleOrderedSet() {
      return (ReversibleOrderedSet<T>) EMPTY_ROS;
   }

   @SuppressWarnings("unchecked")
   public static final <K, V> BidirectionalMap<K, V> emptyBidirectionalMap() {
      return (BidirectionalMap<K, V>) EMPTY_BIDI_MAP;
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
}
