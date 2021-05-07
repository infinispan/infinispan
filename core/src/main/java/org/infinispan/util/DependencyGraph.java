package org.infinispan.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;

import net.jcip.annotations.ThreadSafe;

/**
 *  Graph to track dependencies between objects
 *
 * @author gustavonalle
 * @since 7.0
 */
@ThreadSafe
public final class DependencyGraph<T> {

   private final Map<T, Set<T>> outgoingEdges = new HashMap<>();
   private final Map<T, Set<T>> incomingEdges = new HashMap<>();

   private final StampedLock lock = new StampedLock();

   /**
    * Calculates a topological sort of the graph, in linear time
    *
    * @return List<T> elements sorted respecting dependencies
    * @throws CyclicDependencyException if cycles are present in the graph and thus no topological sort is possible
    */
   public List<T> topologicalSort() throws CyclicDependencyException {
      long stamp = lock.readLock();
      try {
         ArrayList<T> result = new ArrayList<>();
         Deque<T> noIncomingEdges = new ArrayDeque<>();

         Map<T, Integer> temp = new HashMap<>();
         for (Map.Entry<T, Set<T>> incoming : incomingEdges.entrySet()) {
            int size = incoming.getValue().size();
            T key = incoming.getKey();
            temp.put(key, size);
            if (size == 0) {
               noIncomingEdges.add(key);
            }
         }

         while (!noIncomingEdges.isEmpty()) {
            T n = noIncomingEdges.poll();
            result.add(n);
            temp.remove(n);
            Set<T> elements = outgoingEdges.get(n);
            if (elements != null) {
               for (T m : elements) {
                  Integer count = temp.get(m);
                  temp.put(m, --count);
                  if (count == 0) {
                     noIncomingEdges.add(m);
                  }
               }
            }
         }
         if (!temp.isEmpty()) {
            throw new CyclicDependencyException("Cycle detected");
         } else {
            return result;
         }
      } finally {
         lock.unlockRead(stamp);
      }
   }

   /**
    * Add a dependency between two elements
    * @param from From element
    * @param to To element
    */
   public void addDependency(T from, T to) {
      if (from == null || to == null || from.equals(to)) {
         throw new IllegalArgumentException("Invalid parameters");
      }
      long stamp = lock.writeLock();
      try {
         if (addOutgoingEdge(from, to)) {
            addIncomingEdge(to, from);
         }
      } finally {
         lock.unlockWrite(stamp);
      }
   }

   /**
    * Remove a dependency
    * @param from From element
    * @param to To element
    * @throws java.lang.IllegalArgumentException if either to or from don't exist
    */
   public void removeDependency(T from, T to) {
      long stamp = lock.writeLock();
      try {
         Set<T> dependencies = outgoingEdges.get(from);
         if (dependencies == null || !dependencies.contains(to)) {
            throw new IllegalArgumentException("Inexistent dependency");
         }
         dependencies.remove(to);
         incomingEdges.get(to).remove(from);
      } finally {
         lock.unlockWrite(stamp);
      }
   }

   public void clearAll() {
      long stamp = lock.writeLock();
      try {
         outgoingEdges.clear();
         incomingEdges.clear();
      } finally {
         lock.unlockWrite(stamp);
      }
   }

   private void addIncomingEdge(T to, T from) {
      Set<T> incoming = incomingEdges.get(to);
      if (incoming == null) {
         incomingEdges.put(to, newInitialSet(from));
      } else {
         incoming.add(from);
      }
      if (!incomingEdges.containsKey(from)) {
         incomingEdges.put(from, new HashSet<>());
      }
   }

   private boolean addOutgoingEdge(T from, T to) {
      Set<T> outgoing = outgoingEdges.get(from);
      if (outgoing == null) {
         outgoingEdges.put(from, newInitialSet(to));
         return true;
      }
      return outgoing.add(to);
   }

   private Set<T> newInitialSet(T element) {
      Set<T> elements = new HashSet<>();
      elements.add(element);
      return elements;
   }

   /**
    * Check if an element is depended on
    *
    * @param element Element stored in the graph
    * @return true if exists any dependency on element
    * @throws java.lang.IllegalArgumentException if element is not present in the graph
    */
   public boolean hasDependent(T element) {
      long stamp = lock.readLock();
      try {
         Set<T> ts = this.incomingEdges.get(element);
         return ts != null && ts.size() > 0;
      } finally {
         lock.unlockRead(stamp);
      }
   }

   /**
    * Return the dependents
    * @param element Element contained in the graph
    * @return list of elements depending on element
    */
   public Set<T> getDependents(T element) {
      long stamp = lock.readLock();
      try {
         Set<T> dependants = this.incomingEdges.get(element);
         if (dependants == null || dependants.isEmpty()) {
            return Collections.emptySet();
         }
         return Collections.unmodifiableSet(this.incomingEdges.get(element));
      } finally {
         lock.unlockRead(stamp);
      }
   }

   /**
    * Remove element from the graph
    *
    * @param element the element
    */
   public void remove(T element) {
      long stamp = lock.writeLock();
      try {
         if (outgoingEdges.remove(element) != null) {
            for (Set<T> values : outgoingEdges.values()) {
               values.remove(element);
            }
         }
         if (incomingEdges.remove(element) != null) {
            for (Set<T> values : incomingEdges.values()) {
               values.remove(element);
            }
         }
      } finally {
         lock.unlockWrite(stamp);
      }
   }
}
