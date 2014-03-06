package org.infinispan.commons.util;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.equivalence.EquivalentHashMap;
import org.infinispan.commons.equivalence.EquivalentHashSet;
import org.infinispan.commons.equivalence.EquivalentLinkedHashMap;
import org.infinispan.commons.util.concurrent.jdk8backported.ConcurrentParallelHashMapV8;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A factory for ConcurrentMaps.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 5.1
 */
public class CollectionFactory {
   
   private static final ConcurrentMapCreator MAP_CREATOR;

   private static interface ConcurrentMapCreator {
      <K, V> ConcurrentMap<K, V> createConcurrentMap();
      <K, V> ConcurrentMap<K, V> createConcurrentMap(int initialCapacity);
      <K, V> ConcurrentMap<K, V> createConcurrentMap(int initialCapacity, int concurrencyLevel);
      <K, V> ConcurrentMap<K, V> createConcurrentParallelMap(int initialCapacity, int concurrencyLevel);
      <K, V> ConcurrentMap<K, V> createConcurrentMap(int initialCapacity, float loadFactor, int concurrencyLevel);
   }

   private static class JdkConcurrentMapCreator implements ConcurrentMapCreator {

      @Override
      public <K, V> ConcurrentMap<K, V> createConcurrentMap() {
         return new ConcurrentHashMap<K, V>();
      }

      @Override
      public <K, V> ConcurrentMap<K, V> createConcurrentMap(int initialCapacity) {
         return new ConcurrentHashMap<K, V>(initialCapacity);
      }

      @Override
      public <K, V> ConcurrentMap<K, V> createConcurrentMap(int initialCapacity, int concurrencyLevel) {
         return new ConcurrentHashMap<K, V>(initialCapacity, 0.75f, concurrencyLevel);
      }

      @Override
      public <K, V> ConcurrentMap<K, V> createConcurrentMap(int initialCapacity, float loadFactor, int concurrencyLevel) {
         return new ConcurrentHashMap<K, V>(initialCapacity, loadFactor, concurrencyLevel);
      }

      @Override
      public <K, V> ConcurrentMap<K, V> createConcurrentParallelMap(int initialCapacity, int concurrencyLevel) {
         // by the time we baseline on JDK8 this code will be either dropped or adjusted, 
         //for now we need to use ConcurrentParallelHashMapV8
         return new ConcurrentParallelHashMapV8<K, V>(initialCapacity, AnyEquivalence.getInstance(),
               AnyEquivalence.getInstance());
      }
   }

   private static class BackportedV8ConcurrentMapCreator implements ConcurrentMapCreator {

      @Override
      public <K, V> ConcurrentMap<K, V> createConcurrentMap() {
         return new EquivalentConcurrentHashMapV8<K, V>(
               AnyEquivalence.getInstance(), AnyEquivalence.getInstance());
      }

      @Override
      public <K, V> ConcurrentMap<K, V> createConcurrentMap(int initialCapacity) {
         return new EquivalentConcurrentHashMapV8<K, V>(initialCapacity,
               AnyEquivalence.getInstance(), AnyEquivalence.getInstance());
      }

      @Override
      public <K, V> ConcurrentMap<K, V> createConcurrentMap(int initialCapacity, int concurrencyLevel) {
         return new EquivalentConcurrentHashMapV8<K, V>(initialCapacity, 0.75f,
               concurrencyLevel, AnyEquivalence.getInstance(), AnyEquivalence.getInstance());
      }

      @Override
      public <K, V> ConcurrentMap<K, V> createConcurrentMap(int initialCapacity, float loadFactor, int concurrencyLevel) {
         return new EquivalentConcurrentHashMapV8<K, V>(initialCapacity, loadFactor,
               concurrencyLevel, AnyEquivalence.getInstance(), AnyEquivalence.getInstance());
      }

      @Override
      public <K, V> ConcurrentMap<K, V> createConcurrentParallelMap(int initialCapacity, int concurrencyLevel) {
         return new ConcurrentParallelHashMapV8<K, V>(initialCapacity, 0.75f,
               concurrencyLevel, AnyEquivalence.getInstance(), AnyEquivalence.getInstance());
      }
   }
   
   static {
      boolean sunIncompatibleJvm;
      boolean jdk8;
      boolean allowExperimentalMap = Boolean.parseBoolean(System.getProperty("infinispan.unsafe.allow_jdk8_chm", "true"));

      try {
         Class.forName("sun.misc.Unsafe");
         sunIncompatibleJvm = false;
      } catch (ClassNotFoundException e) {
         sunIncompatibleJvm = true;
      }
      
      try {
         Class.forName("java.util.concurrent.atomic.LongAdder");
         jdk8 = true;
      } catch (ClassNotFoundException e) {
         jdk8 = false;
      }

      if (jdk8 || sunIncompatibleJvm || !allowExperimentalMap)
         MAP_CREATOR = new JdkConcurrentMapCreator();
      else
         MAP_CREATOR = new BackportedV8ConcurrentMapCreator();
   }
   
   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap() {
      return MAP_CREATOR.createConcurrentMap();
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(int initCapacity) {
      return MAP_CREATOR.createConcurrentMap(initCapacity);
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(int initCapacity, int concurrencyLevel) {
      return MAP_CREATOR.createConcurrentMap(initCapacity, concurrencyLevel);
   }
   
   public static <K, V> ConcurrentMap<K, V> makeConcurrentParallelMap(int initCapacity, int concurrencyLevel) {
      return MAP_CREATOR.createConcurrentParallelMap(initCapacity, concurrencyLevel);
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(int initCapacity, float loadFactor, int concurrencyLevel) {
      return MAP_CREATOR.createConcurrentMap(initCapacity, loadFactor, concurrencyLevel);
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(
         Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new EquivalentConcurrentHashMapV8<K, V>(keyEq, valueEq);
      else
         return MAP_CREATOR.createConcurrentMap();
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(
         int initCapacity, Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new EquivalentConcurrentHashMapV8<K, V>(initCapacity, keyEq, valueEq);
      else
         return MAP_CREATOR.createConcurrentMap(initCapacity);
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(
         int initCapacity, int concurrencyLevel, Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new EquivalentConcurrentHashMapV8<K, V>(
               initCapacity, concurrencyLevel, keyEq, valueEq);
      else
         return MAP_CREATOR.createConcurrentMap(initCapacity, concurrencyLevel);
   }
   
   public static <K, V> ConcurrentMap<K, V> makeConcurrentParallelMap(
         int initCapacity, int concurrencyLevel, Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new ConcurrentParallelHashMapV8<K, V>(
               initCapacity, concurrencyLevel, keyEq, valueEq);
      else
         return MAP_CREATOR.createConcurrentParallelMap(initCapacity, concurrencyLevel);
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(
         int initCapacity, float loadFactor, int concurrencyLevel,
         Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new EquivalentConcurrentHashMapV8<K, V>(
               initCapacity, loadFactor, concurrencyLevel, keyEq, valueEq);
      else
         return MAP_CREATOR.createConcurrentMap(initCapacity, loadFactor, concurrencyLevel);
   }

   public static <K, V> Map<K, V> makeMap(
         Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new EquivalentHashMap<K, V>(keyEq, valueEq);
      else
         return new HashMap<K, V>();
   }

   public static <K, V> Map<K, V> makeMap(
         int initialCapacity, Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new EquivalentHashMap<K, V>(initialCapacity, keyEq, valueEq);
      else
         return new HashMap<K, V>(initialCapacity);
   }

   public static <K, V> Map<K, V> makeMap(
         Map<? extends K, ? extends V> entries, Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new EquivalentHashMap<K, V>(entries, keyEq, valueEq);
      else
         return new HashMap<K, V>(entries);
   }

   public static <K, V> Map<K, V> makeLinkedMap(int initialCapacity,
         float loadFactor, EquivalentLinkedHashMap.IterationOrder iterationOrder,
         Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new EquivalentLinkedHashMap<K, V>(initialCapacity, loadFactor, iterationOrder, keyEq, valueEq);
      else
         return new LinkedHashMap<K, V>(initialCapacity, loadFactor, iterationOrder.toJdkAccessOrder());
   }

   public static <T> Set<T> makeSet(Equivalence<? super T> entryEq) {
      if (requiresEquivalent(entryEq))
         return new EquivalentHashSet<T>(entryEq);
      else
         return new HashSet<T>();
   }

   public static <T> Set<T> makeSet(int initialCapacity, Equivalence<? super T> entryEq) {
      if (requiresEquivalent(entryEq))
         return new EquivalentHashSet<T>(initialCapacity, entryEq);
      else
         return new HashSet<T>(initialCapacity);
   }

   /**
    * Create a Set backed by the specified array.
    *
    * @param entries the array by which the list will be backed
    * @param <T> type of elements
    * @return a set view of the specified array
    */
   public static <T> Set<T> makeSet(T... entries) {
      return new HashSet<T>(Arrays.asList(entries));
   }

   private static <K, V> boolean requiresEquivalent(
         Equivalence<K> keyEq, Equivalence<V> valueEq) {
      AnyEquivalence<Object> instance = AnyEquivalence.getInstance();
      return keyEq != instance || valueEq != instance;
   }

   private static <T> boolean requiresEquivalent(Equivalence<T> typeEq) {
      return typeEq != AnyEquivalence.getInstance();
   }

}
