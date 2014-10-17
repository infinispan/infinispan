package org.infinispan.commons.util;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

/**
 * Static helpers for Infinispan-specific collections
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class InfinispanCollections {

   private static final Set EMPTY_SET = new EmptySet();
   private static final Map EMPTY_MAP = new EmptyMap();
   private static final List EMPTY_LIST = new EmptyList();

   public static final class EmptySet extends AbstractSet<Object> {

      private static final Iterator<Object> EMPTY_ITERATOR =
            new Iterator<Object>() {
         @Override public boolean hasNext() { return false; }
         @Override public Object next() { throw new NoSuchElementException(); }
         @Override public void remove() { throw new UnsupportedOperationException(); }
      };

      @Override
      public Iterator<Object> iterator() { return EMPTY_ITERATOR; }
      @Override
      public int size() { return 0; }
      @Override
      public boolean contains(Object obj) { return false; }

      public static final class EmptySetExternalizer
            extends AbstractExternalizer<Set> {

         @Override public Integer getId() { return Ids.EMPTY_SET; }
         @Override public void writeObject(ObjectOutput output, Set object) {}
         @Override public Set readObject(ObjectInput input) { return EMPTY_SET; }

         @Override
         @SuppressWarnings("unchecked")
         public Set<Class<? extends Set>> getTypeClasses() {
            return Util.<Class<? extends Set>>asSet(EmptySet.class);
         }
      }
   }

   public static final class EmptyMap extends java.util.AbstractMap<Object,Object> {
      @Override public int size() { return 0; }
      @Override public boolean isEmpty() { return true; }
      @Override public boolean containsKey(Object key) { return false; }
      @Override public boolean containsValue(Object value) { return false; }
      @Override public Object get(Object key) { return null; }
      @Override public Set<Object> keySet() { return emptySet(); }
      @Override public Collection<Object> values() { return emptySet(); }
      @Override public Set<Entry<Object, Object>> entrySet() { return emptySet(); }
      @Override public int hashCode() { return 0; }

      @Override
      public boolean equals(Object o) {
         return (o instanceof Map) && ((Map) o).size() == 0;
      }

      public static final class EmptyMapExternalizer
            extends AbstractExternalizer<Map> {

         @Override public Integer getId() { return Ids.EMPTY_MAP; }
         @Override public void writeObject(ObjectOutput output, Map object) {}
         @Override public Map readObject(ObjectInput input) { return EMPTY_MAP; }

         @Override
         @SuppressWarnings("unchecked")
         public Set<Class<? extends Map>> getTypeClasses() {
            return Util.<Class<? extends Map>>asSet(EmptyMap.class);
         }
      }
   }

   public static final class EmptyList
         extends AbstractList<Object> implements RandomAccess {

      private static final Iterator<Object> EMPTY_ITERATOR =
            new Iterator<Object>() {
               @Override public boolean hasNext() { return false; }
               @Override public Object next() { throw new NoSuchElementException(); }
               @Override public void remove() { throw new UnsupportedOperationException(); }
            };

      @Override public int size() { return 0; }
      @Override public boolean contains(Object obj) { return false; }
      @Override public Iterator<Object> iterator() { return EMPTY_ITERATOR; }

      @Override public Object get(int index) {
         throw new IndexOutOfBoundsException("Index: " + index);
      }

      public static final class EmptyListExternalizer
            extends AbstractExternalizer<List> {

         @Override public Integer getId() { return Ids.EMPTY_LIST; }
         @Override public void writeObject(ObjectOutput output, List object) {}
         @Override public List readObject(ObjectInput input) { return EMPTY_LIST; }

         @Override
         @SuppressWarnings("unchecked")
         public Set<Class<? extends List>> getTypeClasses() {
            return Util.<Class<? extends List>>asSet(EmptyList.class);
         }

      }

   }

   private static final ReversibleOrderedSet<Object> EMPTY_ROS = new EmptyReversibleOrderedSet<Object>();

   @SuppressWarnings("unchecked")
   private static final class EmptyReversibleOrderedSet<E> extends AbstractSet<E> implements ReversibleOrderedSet<E> {

      Iterator<E> it = new Iterator() {

         @Override
         public boolean hasNext() {
            return false;
         }

         @Override
         public E next() {
            throw new NoSuchElementException();
         }

         @Override
         public void remove() {
            throw new UnsupportedOperationException();
         }
      };

      @Override
      public Iterator<E> iterator() {
         return it;
      }

      @Override
      public int size() {
         return 0;
      }

      @Override
      public Iterator<E> reverseIterator() {
         return it;
      }
   }

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
    * A function that converts an entry into a key/value pair for use in a map.
    * @param <K> generated key
    * @param <V> generated value
    * @param <E> entry input
    */
   public static interface MapMakerFunction<K, V, E> {
      /**
       * Transforms the given input into a key/value pair for use in a map
       * @param input instance of the input type
       * @return a Map.Entry parameterized with K and V
       */
      Map.Entry<K, V> transform(E input);
   }

   /**
    * Given a map of well known key/value types, it makes a shallow copy of it
    * while at the same time transforming it's value type to a desired output
    * type. The transformation of the value type is done using a given a
    * function.
    *
    * @param input contains the input key/value pair map
    * @param f function instance to use to transform the value part of the map
    * @param <K> input map's key type
    * @param <V> desired output type of the map's value
    * @param <E> input map's value type
    * @return a shallow copy of the input Map with all its values transformed.
    */
   public static <K, V, E> Map<K, V> transformMapValue(Map<K, E> input, Function<E, V> f) {
      // This screams for a map function! Gimme functional programming pleasee...
      if (input.isEmpty()) return InfinispanCollections.emptyMap();
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

   /**
    * Given a collection, transforms the collection to a map given a {@link MapMakerFunction}
    *
    * @param input contains a collection of type E
    * @param f MapMakerFunction instance to use to transform the collection to a key/value pair
    * @param <K> output map's key type
    * @param <V> output type of the map's value
    * @param <E> input collection's entry type
    * @return a Map with keys and values generated from the input collection
    */
   public static <K, V, E> Map<K, V> transformCollectionToMap(Collection<? extends E> input, MapMakerFunction<K, V, ? super E> f) {
      // This screams for a map function! Gimme functional programming pleasee...
      if (input.isEmpty()) return InfinispanCollections.emptyMap();
      if (input.size() == 1) {
         E single = input.iterator().next();
         Map.Entry<K, V> entry = f.transform(single);
         return singletonMap(entry.getKey(), entry.getValue());
      } else {
         Map<K, V> map = new HashMap<K, V>(input.size());
         for (E e : input) {
            Map.Entry<K, V> entry = f.transform(e);
            map.put(entry.getKey(), entry.getValue());
         }
         return unmodifiableMap(map);
      }
   }

   /**
    * Returns the elements that are present in s1 but which are not present
    * in s2, without changing the contents of neither s1, nor s2.
    *
    * @param s1 first set
    * @param s2 second set
    * @param <E> type of objects in Set
    * @return the elements in s1 that are not in s2
    */
   public static <E> Set<E> difference(Set<? extends E> s1, Set<? extends E> s2) {
      Set<E> copy1 = new HashSet<E>(s1);
      copy1.removeAll(new HashSet<E>(s2));
      return copy1;
   }

   /**
    * Returns the empty set (immutable). Contrary to {@link Collections#emptySet},
    * the set returned returns a constant Iterator, rather than create a
    * brand new one in each iterator call.
    *
    * This set is marshallable using Infinispan's
    * {@link org.jboss.marshalling.Externalizer} framework.
    *
    * @see #EMPTY_SET
    */
   @SuppressWarnings("unchecked")
   public static final <T> Set<T> emptySet() {
      return EMPTY_SET;
   }

   /**
    * Returns the empty map (immutable). Contrary to {@link Collections#emptyMap()},
    * the map returned returns a constant Iterator, rather than create a
    * brand new one in each iterator call.
    *
    * This set is marshallable using Infinispan's
    * {@link org.jboss.marshalling.Externalizer} framework.
    *
    * @see #EMPTY_MAP
    */
   @SuppressWarnings("unchecked")
   public static final <K,V> Map<K,V> emptyMap() {
      return EMPTY_MAP;
   }

   /**
    * Returns the empty list (immutable). Contrary to {@link Collections#emptyList()}},
    * the list returned returns a constant Iterator, rather than create a
    * brand new one in each iterator call.
    *
    * This set is marshallable using Infinispan's
    * {@link org.jboss.marshalling.Externalizer} framework.
    *
    * @see #EMPTY_LIST
    */
   @SuppressWarnings("unchecked")
   public static final <T> List<T> emptyList() {
      return EMPTY_LIST;
   }

   public static final <T> boolean containsAny(Collection<T> haystack, Collection<T> needle) {
      for (T element : needle) {
         if (haystack.contains(element))
            return true;
      }
      return false;
   }
}
