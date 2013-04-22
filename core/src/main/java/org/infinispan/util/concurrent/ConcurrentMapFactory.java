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

package org.infinispan.util.concurrent;

import org.infinispan.util.concurrent.jdk8backported.ConcurrentHashMapV8;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A factory for ConcurrentMaps.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class ConcurrentMapFactory {
   
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
}
