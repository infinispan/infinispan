package org.infinispan.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.infinispan.container.DataContainer;

/**
 * A Set view of keys in a data container, which is read-only and has efficient contains(), unlike some data container
 * ley sets.
 *
 * @author Manik Surtani
 * @since 4.1
 */
public class ReadOnlyDataContainerBackedKeySet implements Set<Object> {

   final DataContainer container;
   Set<Object> keySet;

   public ReadOnlyDataContainerBackedKeySet(DataContainer container) {
      this.container = container;
   }

   @Override
   public int size() {
      return container.size();
   }

   @Override
   public boolean isEmpty() {
      return container.size() == 0;
   }

   @Override
   public boolean contains(Object o) {
      return container.containsKey(o);
   }

   @Override
   public Iterator<Object> iterator() {
      if (keySet == null) keySet = container.keySet();
      return keySet.iterator();
   }

   @Override
   public Object[] toArray() {
      if (keySet == null) keySet = container.keySet();
      return keySet.toArray();
   }

   @Override
   public <T> T[] toArray(T[] a) {
      if (keySet == null) keySet = container.keySet();
      return keySet.toArray(a);
   }

   @Override
   public boolean add(Object o) {
      throw new UnsupportedOperationException("Immutable");
   }

   @Override
   public boolean remove(Object o) {
      throw new UnsupportedOperationException("Immutable");
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      boolean ca = true;
      for (Object o: c) {
         ca = ca && contains(o);
         if (!ca) return false;
      }
      return ca;
   }

   @Override
   public boolean addAll(Collection<?> c) {
      throw new UnsupportedOperationException("Immutable");
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException("Immutable");
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException("Immutable");
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException("Immutable");
   }
}
