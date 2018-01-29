package org.infinispan.commons.util;

import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Static helpers for Infinispan-specific collections
 *
 * @author Manik Surtani
 * @since 4.0
 */
public final class InfinispanCollections {

   private InfinispanCollections() {
      // ensuring non-instantiability
   }

   /**
    * A function that converts a type into another one.
    *
    * @param <E> Input type.
    * @param <T> Output type.
    */
   public interface Function<E, T> {

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
   public interface MapMakerFunction<K, V, E> {
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
      if (input.isEmpty()) return Collections.emptyMap();
      if (input.size() == 1) {
         Map.Entry<K, E> single = input.entrySet().iterator().next();
         return singletonMap(single.getKey(), f.transform(single.getValue()));
      } else {
         Map<K, V> copy = new HashMap<>(input.size());
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
      if (input.isEmpty()) return Collections.emptyMap();
      if (input.size() == 1) {
         E single = input.iterator().next();
         Map.Entry<K, V> entry = f.transform(single);
         return singletonMap(entry.getKey(), entry.getValue());
      } else {
         Map<K, V> map = new HashMap<>(input.size());
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
      Set<E> copy1 = new HashSet<>(s1);
      copy1.removeAll(new HashSet<>(s2));
      return copy1;
   }

   public static <T> boolean containsAny(Collection<T> haystack, Collection<T> needles) {
      for (T element : needles) {
         if (haystack.contains(element))
            return true;
      }
      return false;
   }

   public static <T> void forEach(T[] array, Consumer<T> consumer) {
      final int size = Objects.requireNonNull(array, "Array must be non-null.").length;
      for (int i = 0; i < size; ++i) {
         consumer.accept(array[i]);
      }
   }

   public static void assertNotNullEntries(Map<?, ?> map, String name) {
      Objects.requireNonNull(map, () -> "Map '" + name + "' must be non null.");
      Supplier<String> keySupplier = () -> "Map '" + name + "' contains null key.";
      Supplier<String> valueSupplier = () -> "Map '" + name + "' contains null value.";
      map.forEach((k, v) -> {
         Objects.requireNonNull(k, keySupplier);
         Objects.requireNonNull(v, valueSupplier);
      });
   }

   public static void assertNotNullEntries(Collection<?> collection, String name) {
      Objects.requireNonNull(collection, () -> "Collection '" + name + "' must be non null.");
      Supplier<String> entrySupplier = () -> "Collection '" + name + "' contains null entry.";
      collection.forEach(k -> Objects.requireNonNull(k, entrySupplier));
   }

   public static <K, V> Map<K, V> mergeMaps(Map<K, V> one, Map<K, V> second) {
      if (one == null) {
         return second;
      } else if (second == null) {
         return one;
      } else {
         one.putAll(second);
         return one;
      }
   }

   public static <T> List<T> mergeLists(List<T> one, List<T> second) {
      if (one == null) {
         return second;
      } else if (second == null) {
         return one;
      } else {
         one.addAll(second);
         return one;
      }
   }

   public static Set<Object> toObjectSet(Collection<?> collection) {
      //noinspection unchecked
      return collection instanceof Set ?
            (Set<Object>) collection :
            new HashSet<>(collection);
   }
}
