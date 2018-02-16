package org.infinispan.commons.util;

import javax.annotation.Nonnull;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntFunction;

/**
 * Base for classes that implement hash map by storing keys in one array and values in another.
 * It assumes that all keys that are in the array are contained in the map and that values
 * are on corresponding indices in the map.
 *
 * Does not support null keys nor values.
 *
 * Forces implementation of methods {@link #get(Object)}, {@link #put(Object, Object)}, {@link #remove(Object)}
 */
public abstract class ArrayMap<K, V> extends java.util.AbstractMap<K, V> {
   protected int size;
   protected Object[] keys;
   protected Object[] values;
   protected int modCount;
   private Set<K> keySet;
   private Collection<V> valueCollection;
   private Set<Entry<K, V>> entrySet;

   @Override
   public abstract V get(Object key);

   @Override
   public abstract V put(K key, V value);

   @Override
   public abstract V remove(Object key);

   @Override
   public int size() {
      return size;
   }

   @Override
   public boolean containsValue(@Nonnull Object value) {
      for (int i = 0; i < values.length; ++i) {
         Object v = values[i];
         if (v != null && v.equals(value)) return true;
      }
      return false;
   }

   @Override
   public Set<K> keySet() {
      if (keySet == null) {
         keySet = new KeySet();
      }
      return keySet;
   }

   @Override
   public Collection<V> values() {
      if (valueCollection == null) {
         valueCollection = new Values();
      }
      return valueCollection;
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      if (entrySet == null) {
         entrySet = new EntrySet();
      }
      return entrySet;
   }

   @Override
   public boolean containsKey(Object key) {
      return get(key) != null;
   }

   @Override
   public void clear() {
      size = 0;
      ++modCount;
      Arrays.fill(keys, null);
      Arrays.fill(values, null);
   }

   private class KeySet extends AbstractSet<K> {
      @Override
      public Iterator<K> iterator() {
         return new It<>(i -> (K) keys[i], keys.length, modCount);
      }

      @Override
      public int size() {
         return ArrayMap.this.size();
      }

      @Override
      public void clear() {
         ArrayMap.this.clear();
      }

      @Override
      public boolean contains(Object o) {
         return containsKey(o);
      }

      @Override
      public boolean remove(Object o) {
         return ArrayMap.this.remove(o) != null;
      }
   }

   private class Values extends AbstractCollection<V> {
      @Override
      public Iterator<V> iterator() {
         return new It<>(i -> (V) values[i], values.length, modCount);
      }

      @Override
      public int size() {
         return ArrayMap.this.size();
      }

      @Override
      public void clear() {
         ArrayMap.this.clear();
      }
   }

   private class EntrySet extends AbstractSet<Entry<K, V>> {
      @Override
      public Iterator<Entry<K, V>> iterator() {
         return new It<>(i -> {
            Object key = keys[i];
            return key == null ? null : new SimpleEntry<K, V>((K) key, (V) values[i]);
         }, keys.length, modCount);
      }

      @Override
      public int size() {
         return ArrayMap.this.size();
      }

      @Override
      public void clear() {
         ArrayMap.this.clear();
      }
   }

   private class It<T> implements Iterator<T> {
      private final IntFunction<T> retriever;
      private final int length;
      private final int initialModCount;
      private int last = -1, pos = 0;
      private T next;

      public It(IntFunction<T> retriever, int length, int initialModCount) {
         this.retriever = retriever;
         this.length = length;
         this.initialModCount = initialModCount;
      }

      @Override
      public boolean hasNext() {
         if (initialModCount != modCount) {
            throw new ConcurrentModificationException();
         }
         if (next != null) {
            return true;
         }
         while (pos < length) {
            next = retriever.apply(pos);
            ++pos;
            if (next != null) {
               last = pos -1;
               return true;
            }
         }
         return false;
      }

      @Override
      public T next() {
         if (hasNext()) {
            T tmp = next;
            next = null;
            return tmp;
         } else {
            throw new NoSuchElementException();
         }
      }

      @Override
      public void remove() {
         if (initialModCount != modCount) {
            throw new ConcurrentModificationException();
         }
         keys[last] = null;
         values[last] = null;
      }
   }
}
