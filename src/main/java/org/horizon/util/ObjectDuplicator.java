package org.horizon.util;

import java.util.Collection;
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
         return (Map<K, V>) ((FastCopyHashMap) original).clone();
      if (original instanceof HashMap)
         return (Map<K, V>) ((HashMap) original).clone();
      if (original instanceof TreeMap)
         return (Map<K, V>) ((TreeMap) original).clone();
      return attemptClone(original);
   }

   @SuppressWarnings("unchecked")
   public static <E> Set<E> duplicateSet(Set<E> original) {
      if (original instanceof HashSet)
         return (Set<E>) ((HashSet) original).clone();
      if (original instanceof TreeSet)
         return (Set<E>) ((TreeSet) original).clone();

      return attemptClone(original);
   }

   @SuppressWarnings("unchecked")
   public static <E> ReversibleOrderedSet<E> duplicateReversibleOrderedSet(ReversibleOrderedSet<E> original) {
      if (original instanceof VisitableBidirectionalLinkedHashSet)
         return (ReversibleOrderedSet<E>) ((VisitableBidirectionalLinkedHashSet) original).clone();

      return attemptClone(original);
   }


   @SuppressWarnings("unchecked")
   public static <E> Collection<E> duplicateCollection(Collection<E> original) {
      if (original instanceof HashSet)
         return (Set<E>) ((HashSet) original).clone();
      if (original instanceof TreeSet)
         return (Set<E>) ((TreeSet) original).clone();

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
