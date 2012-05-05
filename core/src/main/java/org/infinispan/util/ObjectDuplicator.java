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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A helper that efficiently duplicates known object types.
 *
 * @author (various)
 * @since 4.0
 */
public class ObjectDuplicator {
   @SuppressWarnings("unchecked")
   public static <K, V> Map<K, V> duplicateMap(Map<K, V> original) {
      if (original instanceof FastCopyHashMap)
         return ((FastCopyHashMap<K, V>) original).clone();
      if (original instanceof HashMap)
         return (Map<K, V>) ((HashMap<K, V>) original).clone();
      if (original instanceof TreeMap)
         return (Map<K, V>) ((TreeMap<K, V>) original).clone();
      if (original.getClass().equals(Collections.emptyMap().getClass()))
         return Collections.emptyMap();
      if (original.getClass().equals(Collections.singletonMap("", "").getClass())) {
         Map.Entry<K, V> e = original.entrySet().iterator().next();
         return Collections.singletonMap(e.getKey(), e.getValue());
      }
      return attemptClone(original);
   }

   @SuppressWarnings("unchecked")
   public static <E> Set<E> duplicateSet(Set<E> original) {
      if (original instanceof HashSet)
         return (Set<E>) ((HashSet<E>) original).clone();
      if (original instanceof TreeSet)
         return (Set<E>) ((TreeSet<E>) original).clone();
      if (original instanceof FastCopyHashMap.EntrySet || original instanceof FastCopyHashMap.KeySet)
         return new HashSet<E>(original);
      if (original.getClass().equals(Collections.emptySet().getClass()))
         return Collections.emptySet();
      if (original.getClass().equals(Collections.singleton("").getClass()))
         return Collections.singleton(original.iterator().next());
      if (original.getClass().getSimpleName().contains("$"))
         return new HashSet<E>(original);

      return attemptClone(original);
   }


   @SuppressWarnings("unchecked")
   public static <E> Collection<E> duplicateCollection(Collection<E> original) {
      if (original instanceof HashSet)
         return (Set<E>) ((HashSet<E>) original).clone();
      if (original instanceof TreeSet)
         return (Set<E>) ((TreeSet<E>) original).clone();

      return attemptClone(original);
   }

   @SuppressWarnings("unchecked")
   private static <T> T attemptClone(T source) {
      if (source instanceof Cloneable) {
         try {
            return (T) source.getClass().getMethod("clone").invoke(source);
         }
         catch (Exception e) {
         }
      }

      return null;
   }
}
