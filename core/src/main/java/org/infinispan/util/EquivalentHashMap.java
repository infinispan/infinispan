/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Custom hash-based map which accepts no null keys nor null values, where
 * equality and hash code calculations are done based on passed
 * {@link org.infinispan.util.Equivalence} function implementations for keys
 * and values, as opposed to relying on their own equals/hashCode/toString
 * implementations. This is handy when using key/values whose mentioned
 * methods cannot be overriden, i.e. arrays, and in situations where users
 * want to avoid using wrapper objects.
 *
 * This hash map implementation is optimised for store/retrieval rather
 * than iteration. Internal node entries are not linked, so responsibility to
 * link them falls on the iterators.
 *
 * @author Galder Zamarreño
 * @since 5.3
 * @see java.util.HashMap
 */
public class EquivalentHashMap<K, V> extends AbstractMap<K, V> {

   private static final int DEFAULT_INITIAL_CAPACITY = 16;

   private static final float DEFAULT_LOAD_FACTOR = 0.75f;

   private static final int MAXIMUM_CAPACITY = 1 << 30;

   private Node<K, V>[] table;

   private int size;

   private int threshold;

   private final float loadFactor;

   private int modCount;

   private final Equivalence<K> keyEq;

   private final Equivalence<V> valueEq;

   @SuppressWarnings("unchecked")
   public EquivalentHashMap(
         Equivalence<K> keyEq, Equivalence<V> valueEq) {
      this(DEFAULT_INITIAL_CAPACITY, keyEq, valueEq);
   }

   @SuppressWarnings("unchecked")
   public EquivalentHashMap(
         int initialCapacity, Equivalence<K> keyEq, Equivalence<V> valueEq) {
      int capacity = 1;
      while (capacity < initialCapacity)
         capacity <<= 1;

      this.loadFactor = DEFAULT_LOAD_FACTOR;
      threshold = (int)(capacity * DEFAULT_LOAD_FACTOR);
      table = new Node[capacity];
      this.keyEq = keyEq;
      this.valueEq = valueEq;
   }

   @SuppressWarnings("unchecked")
   public EquivalentHashMap(
         Map<? extends K, ? extends V> map, Equivalence<K> keyEq, Equivalence<V> valueEq) {
      if (map instanceof EquivalentHashMap) {
         EquivalentHashMap<? extends K, ? extends V> equivalentMap =
               (EquivalentHashMap<? extends K, ? extends V>) map;
         this.table = (Node<K, V>[]) equivalentMap.table.clone();
         this.loadFactor = equivalentMap.loadFactor;
         this.size = equivalentMap.size;
         this.threshold = equivalentMap.threshold;
      } else {
         this.loadFactor = DEFAULT_LOAD_FACTOR;
         init(map.size(), this.loadFactor);
         putAll(map);
      }
      this.keyEq = keyEq;
      this.valueEq = valueEq;
   }

   @SuppressWarnings("unchecked")
   private void init(int initialCapacity, float loadFactor) {
      int c = 1;
      for (; c < initialCapacity; c <<= 1) ;

      this.table = new Node[c];

      threshold = (int) (c * loadFactor);
   }

   @Override
   public int size() {
      return size;
   }

   @Override
   public boolean isEmpty() {
      return size == 0;
   }

   @Override
   public boolean containsKey(Object key) {
      assertKeyNotNull(key);
      int hash = spread(keyEq.hashCode(key));
      int length = table.length;
      int index = index(hash, length);

      for (; ;) {
         Node<K, V> e = table[index];
         if (e == null)
            return false;

         if (e.hash == hash && keyEq.equals(e.key, key))
            return true;

         index = nextIndex(index, length);
      }
   }

   @Override
   public boolean containsValue(Object value) {
      for (Node<K, V> e : table)
         if (e != null && valueEq.equals(e.value, value))
            return true;

      return false;
   }

   @Override
   public V get(Object key) {
      assertKeyNotNull(key);
      int hash = spread(keyEq.hashCode(key));
      int length = table.length;
      int index = index(hash, length);

      for (; ;) {
         Node<K, V> e = table[index];
         if (e == null)
            return null;

         if (e.hash == hash && keyEq.equals(e.key, key))
            return e.value;

         index = nextIndex(index, length);
      }
   }

   @Override
   public V put(K key, V value) {
      assertKeyNotNull(key);
      Node<K, V>[] table = this.table;
      int hash = spread(keyEq.hashCode(key));
      int length = table.length;
      int start = index(hash, length);
      int index = start;

      for (; ;) {
         Node<K, V> e = table[index];
         if (e == null)
            break;

         if (e.hash == hash && keyEq.equals(e.key, key)) {
            table[index] = new Node<K, V>(e.key, e.hash, value);
            return e.value;
         }

         index = nextIndex(index, length);
         if (index == start)
            throw new IllegalStateException("Table is full!");
      }

      modCount++;
      table[index] = new Node<K, V>(key, hash, value);
      if (++size >= threshold)
         resize(length);

      return null;
   }

   @SuppressWarnings("unchecked")
   private void resize(int from) {
      int newLength = from << 1;

      // Can't get any bigger
      if (newLength > MAXIMUM_CAPACITY || newLength <= from)
         return;

      Node<K, V>[] newTable = new Node[newLength];
      Node<K, V>[] old = table;

      for (Node<K, V> e : old) {
         if (e == null)
            continue;

         int index = index(e.hash, newLength);
         while (newTable[index] != null)
            index = nextIndex(index, newLength);

         newTable[index] = e;
      }

      threshold = (int) (loadFactor * newLength);
      table = newTable;
   }

   @Override
   public V remove(Object key) {
      assertKeyNotNull(key);
      Node<K, V>[] table = this.table;
      int length = table.length;
      int hash = spread(keyEq.hashCode(key));
      int start = index(hash, length);

      for (int index = start; ;) {
         Node<K, V> e = table[index];
         if (e == null)
            return null;

         if (e.hash == hash && keyEq.equals(e.key, key)) {
            table[index] = null;
            relocate(index);
            modCount++;
            size--;
            return e.value;
         }

         index = nextIndex(index, length);
         if (index == start)
            return null;
      }
   }

   private void relocate(int start) {
      Node<K, V>[] table = this.table;
      int length = table.length;
      int current = nextIndex(start, length);

      for (; ;) {
         Node<K, V> e = table[current];
         if (e == null)
            return;

         // A Doug Lea variant of Knuth's Section 6.4 Algorithm R.
         // This provides a non-recursive method of relocating
         // entries to their optimal positions once a gap is created.
         int prefer = index(e.hash, length);
         if ((current < prefer && (prefer <= start || start <= current))
               || (prefer <= start && start <= current)) {
            table[start] = e;
            table[current] = null;
            start = current;
         }

         current = nextIndex(current, length);
      }
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map) {
      int size = map.size();
      if (size == 0)
         return;

      if (size > threshold) {
         if (size > MAXIMUM_CAPACITY)
            size = MAXIMUM_CAPACITY;

         int length = table.length;
         for (; length < size; length <<= 1) ;

         resize(length);
      }

      for (Map.Entry<? extends K, ? extends V> e : map.entrySet())
         put(e.getKey(), e.getValue());
   }

   @Override
   public void clear() {
      modCount++;
      Node<K, V>[] table = this.table;
      for (int i = 0; i < table.length; i++)
         table[i] = null;

      size = 0;
   }

   @SuppressWarnings("unchecked")
   public boolean equals(Object o) {
      if (o == this)
         return true;

      if (!(o instanceof Map))
         return false;
      Map<K, V> m = (Map<K, V>) o;
      if (m.size() != size())
         return false;

      try {
         for (Entry<K, V> e : entrySet()) {
            K key = e.getKey();
            V value = e.getValue();
            if (value == null) {
               if (!(m.get(key) == null && m.containsKey(key)))
                  return false;
            } else {
               if (!valueEq.equals(value, m.get(key)))
                  return false;
            }
         }
      } catch (ClassCastException unused) {
         return false;
      } catch (NullPointerException unused) {
         return false;
      }

      return true;
   }

   /* ---------------- Iterating methods and support classes -------------- */

   /**
    * Exported Entry for iterators
    */
   static final class MapEntry<K,V> implements Map.Entry<K,V> {
      final K key; // non-null
      V val;       // non-null
      final EquivalentHashMap<K, V> map;
      MapEntry(K key, V val, EquivalentHashMap<K,V> map) {
         this.key = key;
         this.val = val;
         this.map = map;
      }
      @Override public K getKey() { return key; }
      @Override public V getValue() { return val; }
      @Override public V setValue(V value) {
         if (value == null) throw new NullPointerException();
         V v = val;
         val = value;
         map.put(key, value);
         return v;
      }
      @Override public final int hashCode() {
         return map.keyEq.hashCode(key)
               ^ map.valueEq.hashCode(val);
      }
      @Override public final String toString() {
         return map.keyEq.toString(key) + "="
               + map.valueEq.toString(val);
      }
      @Override public final boolean equals(Object o) {
         Object k, v; Entry<?,?> e;
         return ((o instanceof Entry) &&
            (k = (e = (Entry<?,?>)o).getKey()) != null &&
            (v = e.getValue()) != null &&
            (k == key || map.keyEq.equals(key, k)) &&
            (v == val || map.valueEq.equals(val, v)));
      }
   }

   @Override
   public Set<K> keySet() {
      if (keySet == null) keySet = new KeySet();
      return keySet;
   }

   private final class KeySet extends AbstractSet<K> {
      @Override public Iterator<K> iterator() {
         return new KeyIterator();
      }
      @Override public int size() {
         return size;
      }
      @Override public boolean contains(Object o) {
         return containsKey(o);
      }
      @Override public boolean remove(Object o) {
         int size = size();
         EquivalentHashMap.this.remove(o);
         return size() < size;
      }
      @Override public void clear() {
         EquivalentHashMap.this.clear();
      }
   }

   private final class KeyIterator extends EquivalentHashMapIterator<K> {
      @Override public K next() {
         return nextEntry().getKey();
      }
   }

   private abstract class EquivalentHashMapIterator<E> implements Iterator<E> {
      private int next = 0;
      private int expectedCount = modCount;
      private int current = -1;
      private boolean hasNext;
      Node<K, V> table[] = EquivalentHashMap.this.table;

      public final boolean hasNext() {
         if (hasNext)
            return true;

         Node<K, V> table[] = this.table;
         for (int i = next; i < table.length; i++) {
            if (table[i] != null) {
               next = i;
               return hasNext = true;
            }
         }

         next = table.length;
         return false;
      }

      final Entry<K,V> nextEntry() {
         if (modCount != expectedCount)
            throw new ConcurrentModificationException();

         if (!hasNext && !hasNext())
            throw new NoSuchElementException();

         current = next++;
         hasNext = false;

         Node<K, V> node = table[this.current];
         return new MapEntry<K, V>(
               node.key, node.value, EquivalentHashMap.this);
      }

      public void remove() {
         if (modCount != expectedCount)
            throw new ConcurrentModificationException();

         int current = this.current;

         if (current == -1)
            throw new IllegalStateException();

         // Invalidate current (prevents multiple remove)
         this.current = -1;

         // Start were we relocate
         next = current;

         EquivalentHashMap.this.remove(table[current].key);
         expectedCount = modCount;
      }
   }

   @Override
   public Collection<V> values() {
      if (values == null) values = new Values();
      return values;
   }

   public final class Values extends AbstractCollection<V> {
      @Override public Iterator<V> iterator() {
         return new ValueIterator();
      }
      @Override public int size() {
         return EquivalentHashMap.this.size();
      }
      @Override public boolean contains(Object o) {
         return containsValue(o);
      }
      @Override public void clear() {
         EquivalentHashMap.this.clear();
      }
      @Override public boolean remove(Object o) {
         if (o != null) {
            Iterator<V> it = iterator();
            while (it.hasNext()) {
               if (EquivalentHashMap.this.valueEq.equals(it.next(), o)) {
                  it.remove();
                  return true;
               }
            }
         }
         return false;
      }
   }

   private class ValueIterator extends EquivalentHashMapIterator<V> {
      @Override public V next() {
         return nextEntry().getValue();
      }
   }

   @Override
   public Set<Map.Entry<K, V>> entrySet() {
      if (entrySet == null) entrySet = new EntrySet();
      return entrySet;
   }

   public class EntrySet extends AbstractSet<Map.Entry<K, V>> {
      @Override public Iterator<Map.Entry<K, V>> iterator() {
         return new EntryIterator();
      }

      @Override public boolean contains(Object o) {
         if (!(o instanceof Map.Entry))
            return false;

         Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
         V value = get(entry.getKey());
         return valueEq.equals(value, entry.getValue());
      }

      @Override public void clear() {
         EquivalentHashMap.this.clear();
      }

      @Override public boolean isEmpty() {
         return EquivalentHashMap.this.isEmpty();
      }

      @Override public int size() {
         return EquivalentHashMap.this.size();
      }
   }

   private final class EntryIterator extends EquivalentHashMapIterator<Map.Entry<K,V>> {
      @Override
      public Map.Entry<K, V> next() {
         return nextEntry();
      }
   }

   private static int spread(int hashCode) {
      hashCode ^= (hashCode >>> 20) ^ (hashCode >>> 12);
      return hashCode ^ (hashCode >>> 7) ^ (hashCode >>> 4);
   }

   private static int index(int hashCode, int length) {
      return hashCode & (length - 1);
   }

   private static int nextIndex(int index, int length) {
      index = (index >= length - 1) ? 0 : index + 1;
      return index;
   }

   private static final class Node<K, V> {
      final K key;
      final int hash;
      final V value;

      Node(K key, int hash, V value) {
         this.key = key;
         this.hash = hash;
         this.value = value;
      }
   }

}
