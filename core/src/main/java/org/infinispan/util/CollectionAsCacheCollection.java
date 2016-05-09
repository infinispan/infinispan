package org.infinispan.util;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;

import java.util.Collection;

public class CollectionAsCacheCollection<E> implements CacheCollection<E> {
   private final Collection<E> col;

   public CollectionAsCacheCollection(Collection<E> col) {
      this.col = col;
   }

   @Override
   public int size() {
      return col.size();
   }

   @Override
   public boolean isEmpty() {
      return col.isEmpty();
   }

   @Override
   public boolean contains(Object o) {
      return col.contains(o);
   }

   @Override
   public CloseableIterator<E> iterator() {
      return Closeables.iterator(col.iterator());
   }

   @Override
   public CloseableSpliterator<E> spliterator() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public Object[] toArray() {
      return col.toArray();
   }

   @Override
   public <T> T[] toArray(T[] a) {
      return col.toArray(a);
   }

   @Override
   public boolean add(E e) {
      return col.add(e);
   }

   @Override
   public boolean remove(Object o) {
      return col.remove(o);
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      return col.containsAll(c);
   }

   @Override
   public boolean addAll(Collection<? extends E> c) {
      return col.addAll(c);
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      return col.removeAll(c);
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      return col.retainAll(c);
   }

   @Override
   public void clear() {
      col.clear();
   }

   @Override
   public CacheStream<E> stream() {
      return null;
   }

   @Override
   public CacheStream<E> parallelStream() {
      return null;
   }
}