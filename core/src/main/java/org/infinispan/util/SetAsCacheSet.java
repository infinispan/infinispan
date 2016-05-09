package org.infinispan.util;

import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;

import java.util.Collection;
import java.util.Set;

public class SetAsCacheSet<E> implements CacheSet<E> {
   final Set<E> set;
   final CacheStream<E> stream;

   public SetAsCacheSet(Set<E> set) {
      this.set = set;
      this.stream = null;
   }

   public SetAsCacheSet(Set<E> set, CacheStream<E> stream) {
      this.set = set;
      this.stream = stream;
   }

   @Override
   public CacheStream<E> stream() {
      return stream;
   }

   @Override
   public CacheStream<E> parallelStream() {
      return stream.parallel();
   }

   @Override
   public int size() {
      return set.size();
   }

   @Override
   public boolean isEmpty() {
      return set.isEmpty();
   }

   @Override
   public boolean contains(Object o) {
      return set.contains(o);
   }

   @Override
   public CloseableIterator<E> iterator() {
      return Closeables.iterator(set.iterator());
   }

   @Override
   public Object[] toArray() {
      return set.toArray();
   }

   @Override
   public <T> T[] toArray(T[] a) {
      return set.toArray(a);
   }

   @Override
   public boolean add(E e) {
      return set.add(e);
   }

   @Override
   public boolean remove(Object o) {
      return set.remove(o);
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      return set.containsAll(c);
   }

   @Override
   public boolean addAll(Collection<? extends E> c) {
      return set.addAll(c);
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      return set.removeAll(c);
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      return set.retainAll(c);
   }

   @Override
   public void clear() {
      set.clear();
   }

   @Override
   public CloseableSpliterator<E> spliterator() {
      return null;
   }

   @Override
   public String toString() {
      return "SetAsCacheSet{" +
            "set=" + set +
            '}';
   }
}