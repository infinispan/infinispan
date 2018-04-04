package org.infinispan.commons.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Wrap a {@link HopscotchHashMap} and allow using it as a {@link Set}.
 *
 * @param <E> The element type
 * @since 9.3
 * @author Dan Berindei
 */
public class ImmutableHopscotchHashSet<E> implements Set<E> {
   private static final Object PRESENT = new Object();

   private final HopscotchHashMap<E, Object> map;

   public ImmutableHopscotchHashSet(Collection<E> collection) {
      map = new HopscotchHashMap<>(collection.size());
      for (E e : collection) {
         map.put(e, PRESENT);
      }
   }

   @Override
   public int size() {
      return map.size();
   }

   @Override
   public boolean isEmpty() {
      return map.isEmpty();
   }

   @Override
   public boolean contains(Object o) {
      return map.containsKey(o);
   }

   @Override
   public Iterator<E> iterator() {
      return new Immutables.ImmutableIteratorWrapper<>(map.keySet().iterator());
   }

   @Override
   public void forEach(Consumer<? super E> action) {
      map.keySet().forEach(action);
   }

   @Override
   public Object[] toArray() {
      return map.keySet().toArray();
   }

   @Override
   public <T> T[] toArray(T[] a) {
      return map.keySet().toArray(a);
   }

   @Override
   public boolean add(E e) {
      throw new UnsupportedOperationException("add");
   }

   @Override
   public boolean remove(Object o) {
      throw new UnsupportedOperationException("remove");
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      return map.keySet().containsAll(c);
   }

   @Override
   public boolean addAll(Collection<? extends E> c) {
      throw new UnsupportedOperationException("addAll");
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException("retainAll");
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException("removeAll");
   }

   @Override
   public boolean removeIf(Predicate<? super E> filter) {
      throw new UnsupportedOperationException("removeIf");
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException("clear");
   }

   // Use the default implementation of spliterator(), stream(), and parallelStream()
}
