package org.infinispan.commons.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.commons.equivalence.Equivalence;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * A factory for ConcurrentMaps.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 5.1
 * @deprecated since 10.0, will be removed in future Infinispan versions
 */
@Deprecated
public class CollectionFactory {
   private static final float DEFAULT_LOAD_FACTOR = 0.75f;

   public static int computeCapacity(int expectedSize) {
      return computeCapacity(expectedSize, DEFAULT_LOAD_FACTOR);
   }

   public static int computeCapacity(int expectedSize, float loadFactor) {
      return (int) (expectedSize / loadFactor + 1.0f);
   }


   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap() {
      return new ConcurrentHashMap<>();
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(int initCapacity) {
      return new ConcurrentHashMap<>(initCapacity);
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(int initCapacity, int concurrencyLevel) {
      return new ConcurrentHashMap<>(initCapacity, 0.75f, concurrencyLevel);
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentParallelMap(int initCapacity, int concurrencyLevel) {
      return new ConcurrentHashMap<>(initCapacity);
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(int initCapacity, float loadFactor, int concurrencyLevel) {
      return new ConcurrentHashMap<>(initCapacity, loadFactor, concurrencyLevel);
   }

   public static <K, V> Map<K, V> makeMap() {
      return new HashMap<>();
   }

   public static <K, V> Map<K, V> makeMap(int initialCapacity) {
      return new HashMap<>(initialCapacity);
   }

   public static <K, V> Map<K, V> makeMap(Map<? extends K, ? extends V> entries) {
      return new HashMap<>(entries);
   }

   public static <K, V> Map<K, V> makeLinkedMap(int initialCapacity, float loadFactor, boolean accessOrder) {
      return new LinkedHashMap<>(initialCapacity, loadFactor, accessOrder);
   }

   public static <T> Set<T> makeSet() {
      return new HashSet<>();
   }

   public static <T> Set<T> makeSet(int initialCapacity) {
      return new HashSet<>(initialCapacity);
   }

   /**
    * @deprecated Since 9.0, please use {@link #makeConcurrentMap()} instead.
    */
   @Deprecated
   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(
         Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      return makeConcurrentMap();
   }

   /**
    * @deprecated Since 9.0, please use {@link #makeConcurrentMap(int)} instead.
    */
   @Deprecated
   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(
         int initCapacity, Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      return makeConcurrentMap(initCapacity);
   }

   /**
    * @deprecated Since 9.0, please use {@link #makeConcurrentMap(int, int)} instead.
    */
   @Deprecated
   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(
         int initCapacity, int concurrencyLevel, Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      return makeConcurrentMap(initCapacity, concurrencyLevel);
   }

   /**
    * @deprecated Since 9.0, please use {@link #makeConcurrentParallelMap(int, int)} instead.
    */
   @Deprecated
   public static <K, V> ConcurrentMap<K, V> makeConcurrentParallelMap(
         int initCapacity, int concurrencyLevel, Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      return makeConcurrentParallelMap(initCapacity, concurrencyLevel);
   }

   /**
    * @deprecated Since 9.0, please use {@link #makeConcurrentMap(int, float, int)} instead.
    */
   @Deprecated
   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(
         int initCapacity, float loadFactor, int concurrencyLevel,
         Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      return makeConcurrentMap(initCapacity, loadFactor, concurrencyLevel);
   }

   public static <K, V> ConcurrentMap<K, V> makeBoundedConcurrentMap(int maxSize) {
      Cache<K, V> cache = Caffeine.newBuilder().maximumSize(maxSize).build();
      return cache.asMap();
   }

   /**
    * @deprecated Since 9.0, please use {@link #makeMap()} instead.
    */
   @Deprecated
   public static <K, V> Map<K, V> makeMap(
         Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      return new HashMap<>();
   }

   /**
    * @deprecated Since 9.0, please use {@link #makeMap(int)} instead.
    */
   @Deprecated
   public static <K, V> Map<K, V> makeMap(
         int initialCapacity, Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      return new HashMap<>(initialCapacity);
   }

   /**
    * @deprecated Since 9.0, please use {@link #makeMap(Map)} instead.
    */
   @Deprecated
   public static <K, V> Map<K, V> makeMap(
         Map<? extends K, ? extends V> entries, Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      return new HashMap<>(entries);
   }

   /**
    * @deprecated Since 9.0, please use {@link #makeSet(int)} instead.
    */
   @Deprecated
   public static <K, V> Map<K, V> makeLinkedMap(int initialCapacity,
         float loadFactor, boolean accessOrder,
         Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      return new LinkedHashMap<>(initialCapacity, loadFactor, accessOrder);
   }

   /**
    * @deprecated Since 9.0, please use {@link #makeSet()} instead.
    */
   @Deprecated
   public static <T> Set<T> makeSet(Equivalence<? super T> entryEq) {
      return new HashSet<>();
   }

   /**
    * @deprecated Since 9.0, please use {@link #makeSet(int)} instead.
    */
   @Deprecated
   public static <T> Set<T> makeSet(int initialCapacity, Equivalence<? super T> entryEq) {
      return new HashSet<>(initialCapacity);
   }

   /**
    * Create a Set backed by the specified array.
    *
    * @param entries the array by which the list will be backed
    * @param <T> type of elements
    * @return a set view of the specified array
    */
   @SafeVarargs
   public static <T> Set<T> makeSet(T... entries) {
      return new HashSet<>(Arrays.asList(entries));
   }
}
