package org.infinispan.commons.equivalence;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.infinispan.commons.util.AbstractMap;

/**
 * Custom hash-based map which accepts no null keys nor null values, where
 * equality and hash code calculations are done based on passed
 * {@link org.infinispan.commons.equivalence.Equivalence} function implementations for keys
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

   int size;

   private int threshold;

   private final float loadFactor;

   int modCount;

   private final Equivalence<? super K> keyEq;

   private final Equivalence<? super V> valueEq;

   @SuppressWarnings("unchecked")
   public EquivalentHashMap(
         Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      this(DEFAULT_INITIAL_CAPACITY, keyEq, valueEq);
   }

   @SuppressWarnings("unchecked")
   public EquivalentHashMap(
         int initialCapacity, Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      this(initialCapacity, DEFAULT_LOAD_FACTOR, keyEq, valueEq);
   }

   @SuppressWarnings("unchecked")
   public EquivalentHashMap(
         int initialCapacity, float loadFactor,
         Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      int capacity = 1;
      while (capacity < initialCapacity)
         capacity <<= 1;

      this.loadFactor = loadFactor;
      threshold = (int)(capacity * loadFactor);
      table = new Node[capacity];
      this.keyEq = keyEq;
      this.valueEq = valueEq;
   }

   @SuppressWarnings("unchecked")
   public EquivalentHashMap(
         Map<? extends K, ? extends V> map, Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
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

      Node<K, V> e = table[index];
      while (e != null) {

         if (e.hash == hash && keyEq.equals(e.key, key))
            return true;

         e = e.next;
      }
      return false;
   }

   @Override
   public boolean containsValue(Object value) {
      for (Node<K, V> e : table) {
         for (; e != null; e = e.next) {
            if (valueEq.equals(e.value, value)) {
               return true;
            }
         }
      }

      return false;
   }

   @Override
   public V get(Object key) {
      Node<K, V> n = getNode(key);
      return n == null ? null : n.value;
   }

   <T> T getNode(Object key) {
      assertKeyNotNull(key);
      int hash = spread(keyEq.hashCode(key));
      int length = table.length;
      int index = index(hash, length);

      Node<K, V> e = table[index];
      while (e != null) {
         if (e.hash == hash && keyEq.equals(e.key, key))
            return (T) e;

         e = e.next;
      }
      return null;
   }

   @Override
   public V put(K key, V value) {
      assertKeyNotNull(key);
      Node<K, V>[] table = this.table;
      int hash = spread(keyEq.hashCode(key));
      int length = table.length;
      int index = index(hash, length);

      Node<K, V> e = table[index];
      while (e != null) {
         if (e.hash == hash && keyEq.equals(e.key, key)) {
            V prevValue = e.value;
            e.setValue(value, this);
            return prevValue;
         }
         e = e.next;
      }

      modCount++;
      addEntry(index, key, value, hash);

      return null;
   }

   void addEntry(int index, K key, V value, int hash) {
      if (++size >= threshold && table[index] != null) {
         resize(table.length << 1);
         index = index(hash, table.length);
      }
      Node<K, V> currentNode = table[index];
      table[index] = createNode(key, value, hash, currentNode);
   }

   Node<K, V> createNode(K key, V value, int hash, Node<K, V> currentNode) {
      return new Node<K, V>(key, hash, value, currentNode);
   }

   @SuppressWarnings("unchecked")
   void resize(int newCapacity) {
      Node<K, V>[] oldTable = table;
      int oldCapacity = oldTable.length;
      if (oldCapacity == MAXIMUM_CAPACITY) {
         threshold = Integer.MAX_VALUE;
         return;
      }

      Node<K, V>[] newTable = new Node[newCapacity];
      transfer(newTable);
      table = newTable;
      threshold = (int)Math.min(newCapacity * loadFactor, MAXIMUM_CAPACITY + 1);
   }

   void transfer(Node<K, V>[] newTable) {
      int newCapacity = newTable.length;
      for (Node<K,V> e : table) {
         while(e != null) {
            Node<K, V> next = e.next;
            int i = index(spread(keyEq.hashCode(e.key)), newCapacity);
            e.next = newTable[i];
            newTable[i] = e;
            e = next;
         }
      }
   }

   @Override
   public V remove(Object key) {
      Node<K, V> prevNode = removeNode(key);
      return prevNode == null ? null : prevNode.value;
   }

   <T> T removeNode(Object key) {
      assertKeyNotNull(key);
      Node<K, V>[] table = this.table;
      int length = table.length;
      int hash = spread(keyEq.hashCode(key));
      int index = index(hash, length);

      Node<K, V> e = table[index];
      Node<K, V> prevE = null;
      while (e != null) {
         if (e.hash == hash && keyEq.equals(e.key, key)) {
            if (prevE != null) {
               prevE.next = e.next;
            } else {
               table[index] = e.next;
            }
            modCount++;
            size--;
            return (T) e;
         }

         prevE = e;
         e = e.next;
      }
      return null;
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

   public Equivalence<? super K> getKeyEquivalence() {
      return keyEq;
   }

   public Equivalence<? super V> getValueEquivalence() {
      return valueEq;
   }

   /* ---------------- Iterating methods and support classes -------------- */

   /**
    * Exported Entry for iterators
    */
   static class MapEntry<K,V> implements Map.Entry<K,V> {
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

   Iterator<K> newKeyIterator()   {
      return new KeyIterator();
   }

   Iterator<V> newValueIterator()   {
      return new ValueIterator();
   }

   Iterator<Map.Entry<K,V>> newEntryIterator()   {
      return new EntryIterator();
   }

   private final class KeySet extends AbstractSet<K> {
      @Override public Iterator<K> iterator() {
         return newKeyIterator();
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
      Node<K,V> next;        // next entry to return
      int expectedCount;   // For fast-fail
      int index;              // current slot
      Node<K,V> current;     // current entry

      EquivalentHashMapIterator() {
         expectedCount = modCount;
         if (size > 0) { // advance to first entry
            Node<K, V>[] t = table;
            while (index < t.length && (next = t[index++]) == null)
               ;
         }
      }

      public final boolean hasNext() {
         return next != null;
      }

      final Entry<K,V> nextEntry() {
         if (modCount != expectedCount)
            throw new ConcurrentModificationException();
         Node<K,V> e = next;
         if (e == null)
            throw new NoSuchElementException();

         if ((next = e.next) == null) {
            Node<K, V>[] t = table;
            while (index < t.length && (next = t[index++]) == null)
               ;
         }
         current = e;
         return new MapEntry<K, V>(e.key, e.value, EquivalentHashMap.this);
      }

      public void remove() {
         if (modCount != expectedCount)
            throw new ConcurrentModificationException();
         if (current == null)
            throw new IllegalStateException();
         Object k = current.key;
         current = null;
         removeNode(k);
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
         return newValueIterator();
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
         return newEntryIterator();
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

   protected static class Node<K, V> implements Entry<K, V>  {
      final K key;
      final int hash;
      V value;
      Node<K, V> next;

      protected Node(K key, int hash, V value, Node<K, V> next) {
         this.key = key;
         this.hash = hash;
         this.value = value;
         this.next = next;
      }

      @Override
      public K getKey() {
         return key;
      }

      @Override
      public V getValue() {
         return value;
      }

      @Override
      public V setValue(V value) {
         V prevValue = this.value;
         this.value = value;
         return prevValue;
      }

      protected V setValue(V value, EquivalentHashMap<K, V> map) {
         return setValue(value);
      }
   }

}
