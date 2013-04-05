/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.util;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.util.concurrent.jdk8backported.ConcurrentHashMapV8;
import org.infinispan.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;

import java.util.HashMap;
import java.util.HashSet;
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
   }

   private static class BackportedV8ConcurrentMapCreator implements ConcurrentMapCreator {

      @Override
      public <K, V> ConcurrentMap<K, V> createConcurrentMap() {
         return new ConcurrentHashMapV8<K, V>();
      }

      @Override
      public <K, V> ConcurrentMap<K, V> createConcurrentMap(int initialCapacity) {
         return new ConcurrentHashMapV8<K, V>(initialCapacity);
      }

      @Override
      public <K, V> ConcurrentMap<K, V> createConcurrentMap(int initialCapacity, int concurrencyLevel) {
         return new ConcurrentHashMapV8<K, V>(initialCapacity, 0.75f, concurrencyLevel);
      }

      @Override
      public <K, V> ConcurrentMap<K, V> createConcurrentMap(int initialCapacity, float loadFactor, int concurrencyLevel) {
         return new ConcurrentHashMapV8<K, V>(initialCapacity, loadFactor, concurrencyLevel);
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

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(int initCapacity, float loadFactor, int concurrencyLevel) {
      return MAP_CREATOR.createConcurrentMap(initCapacity, loadFactor, concurrencyLevel);
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(
         Equivalence<K> keyEq, Equivalence<V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new EquivalentConcurrentHashMapV8<K, V>(keyEq, valueEq);
      else
         return MAP_CREATOR.createConcurrentMap();
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(
         int initCapacity, Equivalence<K> keyEq, Equivalence<V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new EquivalentConcurrentHashMapV8<K, V>(initCapacity, keyEq, valueEq);
      else
         return MAP_CREATOR.createConcurrentMap(initCapacity);
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(
         int initCapacity, int concurrencyLevel, Equivalence<K> keyEq, Equivalence<V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new EquivalentConcurrentHashMapV8<K, V>(
               initCapacity, concurrencyLevel, keyEq, valueEq);
      else
         return MAP_CREATOR.createConcurrentMap(initCapacity, concurrencyLevel);
   }

   public static <K, V> ConcurrentMap<K, V> makeConcurrentMap(
         int initCapacity, float loadFactor, int concurrencyLevel,
         Equivalence<K> keyEq, Equivalence<V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new EquivalentConcurrentHashMapV8<K, V>(
               initCapacity, loadFactor, concurrencyLevel, keyEq, valueEq);
      else
         return MAP_CREATOR.createConcurrentMap(initCapacity, loadFactor, concurrencyLevel);
   }

   public static <K, V> Map<K, V> makeMap(
         Equivalence<K> keyEq, Equivalence<V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new EquivalentHashMap<K, V>(keyEq, valueEq);
      else
         return new HashMap<K, V>();
   }

   public static <K, V> Map<K, V> makeMap(
         int initialCapacity, Equivalence<K> keyEq, Equivalence<V> valueEq) {
      if (requiresEquivalent(keyEq, valueEq))
         return new EquivalentHashMap<K, V>(initialCapacity, keyEq, valueEq);
      else
         return new HashMap<K, V>(initialCapacity);
   }

   public static <T> Set<T> makeSet(Equivalence<T> entryEq) {
      if (requiresEquivalent(entryEq))
         return new EquivalentHashSet<T>(entryEq);
      else
         return new HashSet<T>();
   }

   public static <T> Set<T> makeSet(int initialCapacity, Equivalence<T> entryEq) {
      if (requiresEquivalent(entryEq))
         return new EquivalentHashSet<T>(initialCapacity, entryEq);
      else
         return new HashSet<T>(initialCapacity);
   }

   private static <K, V> boolean requiresEquivalent(
         Equivalence<K> keyEq, Equivalence<V> valueEq) {
      return keyEq != AnyEquivalence.OBJECT || valueEq != AnyEquivalence.OBJECT;
   }

   private static <T> boolean requiresEquivalent(Equivalence<T> typeEq) {
      return typeEq != AnyEquivalence.OBJECT;
   }

}
