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

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Similar to the JDK's {@link java.util.LinkedHashMap} except that this version makes use of the fact that entries are
 * bidirectionally linked and can hence be navigated either from the start <i>or</i> from the end.  It exposes such
 * navigability by overriding {@link java.util.Map#keySet()} and {@link java.util.Map#entrySet()} to return {@link
 * ReversibleOrderedSet} rather than a standard JDK {@link java.util.Set}.  {@link ReversibleOrderedSet}s allow you to
 * access 2 iterators: one that iterates from start to end, as usual, and a reversed one that iterates from end to start
 * instead.
 * <p/>
 * Unlike the JDK {@link java.util.LinkedHashMap}, this implementation does not support null keys.
 * <p/>
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class BidirectionalLinkedHashMap<K, V> extends AbstractMap<K, V> implements BidirectionalMap<K, V>, Cloneable {

   /**
    * The head of the doubly linked list.
    */
   private transient LinkedEntry<K, V> header;

   /**
    * The iteration ordering method for this linked hash map: <tt>true</tt> for access-order, <tt>false</tt> for
    * insertion-order.
    *
    * @serial
    */
   private final boolean accessOrder;


   /**
    * The default initial capacity - MUST be a power of two.
    */
   static final int DEFAULT_INITIAL_CAPACITY = 16;

   /**
    * The maximum capacity, used if a higher value is implicitly specified by either of the constructors with arguments.
    * MUST be a power of two <= 1<<30.
    */
   static final int MAXIMUM_CAPACITY = 1 << 30;

   /**
    * The load factor used when none specified in constructor.
    */
   static final float DEFAULT_LOAD_FACTOR = 0.75f;

   /**
    * The table, resized as necessary. Length MUST Always be a power of two.
    */
   transient LinkedEntry[] table;

   /**
    * The number of key-value mappings contained in this map.
    */
   transient int size;

   /**
    * The next size value at which to resize (capacity * load factor).
    *
    * @serial
    */
   int threshold;

   /**
    * The load factor for the hash table.
    *
    * @serial
    */
   final float loadFactor;

   /**
    * The number of times this HashMap has been structurally modified Structural modifications are those that change the
    * number of mappings in the HashMap or otherwise modify its internal structure (e.g., rehash).  This field is used
    * to make iterators on Collection-views of the HashMap fail-fast.  (See ConcurrentModificationException).
    */
   transient volatile int modCount;

   /**
    * Constructs an empty <tt>HashMap</tt> with the specified initial capacity and load factor.
    *
    * @param initialCapacity the initial capacity
    * @param loadFactor      the load factor
    * @throws IllegalArgumentException if the initial capacity is negative or the load factor is non-positive
    */
   public BidirectionalLinkedHashMap(int initialCapacity, float loadFactor) {
      this(initialCapacity, loadFactor, false);
   }

   /**
    * Constructs an empty <tt>LinkedHashMap</tt> instance with the specified initial capacity, load factor and ordering
    * mode.
    *
    * @param initialCapacity the initial capacity
    * @param loadFactor      the load factor
    * @param accessOrder     the ordering mode - <tt>true</tt> for access-order, <tt>false</tt> for insertion-order
    * @throws IllegalArgumentException if the initial capacity is negative or the load factor is non-positive
    */
   public BidirectionalLinkedHashMap(int initialCapacity, float loadFactor, boolean accessOrder) {
      if (initialCapacity < 0)
         throw new IllegalArgumentException("Illegal initial capacity: " +
               initialCapacity);
      if (initialCapacity > MAXIMUM_CAPACITY)
         initialCapacity = MAXIMUM_CAPACITY;
      if (loadFactor <= 0 || Float.isNaN(loadFactor))
         throw new IllegalArgumentException("Illegal load factor: " +
               loadFactor);

      // Find a power of 2 >= initialCapacity
      int capacity = 1;
      while (capacity < initialCapacity)
         capacity <<= 1;

      this.loadFactor = loadFactor;
      threshold = (int) (capacity * loadFactor);
      table = new LinkedEntry[capacity];
      this.accessOrder = accessOrder;
      init();
   }

   /**
    * Constructs an empty <tt>HashMap</tt> with the specified initial capacity and the default load factor (0.75).
    *
    * @param initialCapacity the initial capacity.
    * @throws IllegalArgumentException if the initial capacity is negative.
    */
   public BidirectionalLinkedHashMap(int initialCapacity) {
      this(initialCapacity, DEFAULT_LOAD_FACTOR);
   }

   /**
    * Constructs an empty <tt>HashMap</tt> with the default initial capacity (16) and the default load factor (0.75).
    */
   public BidirectionalLinkedHashMap() {
      this.loadFactor = DEFAULT_LOAD_FACTOR;
      threshold = (int) (DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
      table = new LinkedEntry[DEFAULT_INITIAL_CAPACITY];
      accessOrder = false;
      init();
   }

   /**
    * Constructs a new <tt>HashMap</tt> with the same mappings as the specified <tt>Map</tt>.  The <tt>HashMap</tt> is
    * created with default load factor (0.75) and an initial capacity sufficient to hold the mappings in the specified
    * <tt>Map</tt>.
    *
    * @param m the map whose mappings are to be placed in this map
    * @throws NullPointerException if the specified map is null
    */
   public BidirectionalLinkedHashMap(Map<? extends K, ? extends V> m) {
      this(Math.max((int) (m.size() / DEFAULT_LOAD_FACTOR) + 1,
                    DEFAULT_INITIAL_CAPACITY), DEFAULT_LOAD_FACTOR, false);
      putAllForCreate(m);
   }

   // internal utilities

   /**
    * Returns index for hash code h.
    */
   static int indexFor(int h, int length) {
      return h & (length - 1);
   }

   /**
    * Returns the number of key-value mappings in this map.
    *
    * @return the number of key-value mappings in this map
    */
   //@Override
   public int size() {
      return size;
   }

   /**
    * Returns <tt>true</tt> if this map contains no key-value mappings.
    *
    * @return <tt>true</tt> if this map contains no key-value mappings
    */
   //@Override
   public boolean isEmpty() {
      return size == 0;
   }

   /**
    * Returns <tt>true</tt> if this map contains a mapping for the specified key.
    *
    * @param key The key whose presence in this map is to be tested
    * @return <tt>true</tt> if this map contains a mapping for the specified key.
    */
   public boolean containsKey(Object key) {
      return getEntry(key) != null;
   }

   /**
    * Returns the entry associated with the specified key in the HashMap.  Returns null if the HashMap contains no
    * mapping for the key.
    */
   final LinkedEntry<K, V> getEntry(Object key) {
      int hash = (key == null) ? 0 : hash(key);
      for (LinkedEntry<K, V> e = table[indexFor(hash, table.length)];
           e != null;
           e = e.next) {
         Object k;
         if (e.hash == hash &&
               ((k = e.key) == key || (key != null && key.equals(k))))
            return e;
      }
      return null;
   }


   /**
    * Associates the specified value with the specified key in this map. If the map previously contained a mapping for
    * the key, the old value is replaced.
    *
    * @param key   key with which the specified value is to be associated
    * @param value value to be associated with the specified key
    * @return the previous value associated with <tt>key</tt>, or <tt>null</tt> if there was no mapping for
    *         <tt>key</tt>. (A <tt>null</tt> return can also indicate that the map previously associated <tt>null</tt>
    *         with <tt>key</tt>.)
    */
   public V put(K key, V value) {
      assertKeyNotNull(key);
      int hash = hash(key);
      int i = indexFor(hash, table.length);
      for (LinkedEntry<K, V> e = table[i]; e != null; e = e.next) {
         Object k;
         if (e.hash == hash && ((k = e.key) == key || key.equals(k))) {
            V oldValue = e.value;
            e.value = value;
            e.recordAccess(this);
            return oldValue;
         }
      }

      modCount++;
      addEntry(hash, key, value, i);
      return null;
   }

   /**
    * This method is used instead of put by constructors and pseudo constructors (clone, readObject).  It does not resize
    * the table, check for co-modification, etc.  It calls createEntry rather than addEntry.
    */
   private void putForCreate(K key, V value) {
      int hash = (key == null) ? 0 : hash(key);
      int i = indexFor(hash, table.length);

      /**
       * Look for preexisting entry for key.  This will never happen for
       * clone or deserialize.  It will only happen for construction if the
       * input Map is a sorted map whose ordering is inconsistent w/ equals.
       */
      for (LinkedEntry<K, V> e = table[i]; e != null; e = e.next) {
         Object k;
         if (e.hash == hash &&
               ((k = e.key) == key || (key != null && key.equals(k)))) {
            e.value = value;
            return;
         }
      }

      createEntry(hash, key, value, i);
   }

   private void putAllForCreate(Map<? extends K, ? extends V> m) {
      for (Entry<? extends K, ? extends V> e : m.entrySet()) putForCreate(e.getKey(), e.getValue());
   }

   /**
    * Rehashes the contents of this map into a new array with a larger capacity.  This method is called automatically
    * when the number of keys in this map reaches its threshold.
    * <p/>
    * If current capacity is MAXIMUM_CAPACITY, this method does not resize the map, but sets threshold to
    * Integer.MAX_VALUE. This has the effect of preventing future calls.
    *
    * @param newCapacity the new capacity, MUST be a power of two; must be greater than current capacity unless current
    *                    capacity is MAXIMUM_CAPACITY (in which case value is irrelevant).
    */
   void resize(int newCapacity) {
      LinkedEntry[] oldTable = table;
      int oldCapacity = oldTable.length;
      if (oldCapacity == MAXIMUM_CAPACITY) {
         threshold = Integer.MAX_VALUE;
         return;
      }

      LinkedEntry[] newTable = new LinkedEntry[newCapacity];
      transfer(newTable);
      table = newTable;
      threshold = (int) (newCapacity * loadFactor);
   }

   /**
    * Copies all of the mappings from the specified map to this map. These mappings will replace any mappings that this
    * map had for any of the keys currently in the specified map.
    *
    * @param m mappings to be stored in this map
    * @throws NullPointerException if the specified map is null
    */
   public void putAll(Map<? extends K, ? extends V> m) {
      int numKeysToBeAdded = m.size();
      if (numKeysToBeAdded == 0)
         return;

      /*
      * Expand the map if the map if the number of mappings to be added
      * is greater than or equal to threshold.  This is conservative; the
      * obvious condition is (m.size() + size) >= threshold, but this
      * condition could result in a map with twice the appropriate capacity,
      * if the keys to be added overlap with the keys already in this map.
      * By using the conservative calculation, we subject ourself
      * to at most one extra resize.
      */
      if (numKeysToBeAdded > threshold) {
         int targetCapacity = (int) (numKeysToBeAdded / loadFactor + 1);
         if (targetCapacity > MAXIMUM_CAPACITY)
            targetCapacity = MAXIMUM_CAPACITY;
         int newCapacity = table.length;
         while (newCapacity < targetCapacity)
            newCapacity <<= 1;
         if (newCapacity > table.length)
            resize(newCapacity);
      }

      for (Entry<? extends K, ? extends V> e : m.entrySet()) put(e.getKey(), e.getValue());
   }

   /**
    * Removes the mapping for the specified key from this map if present.
    *
    * @param key key whose mapping is to be removed from the map
    * @return the previous value associated with <tt>key</tt>, or <tt>null</tt> if there was no mapping for
    *         <tt>key</tt>. (A <tt>null</tt> return can also indicate that the map previously associated <tt>null</tt>
    *         with <tt>key</tt>.)
    */
   public V remove(Object key) {
      assertKeyNotNull(key);
      LinkedEntry<K, V> e = removeEntryForKey(key);
      return (e == null ? null : e.value);
   }

   /**
    * Removes and returns the entry associated with the specified key in the HashMap.  Returns null if the HashMap
    * contains no mapping for this key.
    */
   final LinkedEntry<K, V> removeEntryForKey(Object key) {
      int hash = hash(key);
      int i = indexFor(hash, table.length);
      LinkedEntry<K, V> prev = table[i];
      LinkedEntry<K, V> e = prev;

      while (e != null) {
         LinkedEntry<K, V> next = e.next;
         Object k;
         if (e.hash == hash &&
               ((k = e.key) == key || (key != null && key.equals(k)))) {
            modCount++;
            size--;
            if (prev == e)
               table[i] = next;
            else
               prev.next = next;
            e.recordRemoval(this);
            return e;
         }
         prev = e;
         e = next;
      }

      return e;
   }

   /**
    * Special version of remove for EntrySet.
    */
   final LinkedEntry<K, V> removeMapping(Object o) {
      if (!(o instanceof Map.Entry))
         return null;

      Map.Entry<K, V> entry = (Map.Entry<K, V>) o;
      Object key = entry.getKey();
      int hash = (key == null) ? 0 : hash(key);
      int i = indexFor(hash, table.length);
      LinkedEntry<K, V> prev = table[i];
      LinkedEntry<K, V> e = prev;

      while (e != null) {
         LinkedEntry<K, V> next = e.next;
         if (e.hash == hash && e.equals(entry)) {
            modCount++;
            size--;
            if (prev == e)
               table[i] = next;
            else
               prev.next = next;
            e.recordRemoval(this);
            return e;
         }
         prev = e;
         e = next;
      }

      return e;
   }

   /**
    * Removes all of the mappings from this map. The map will be empty after this call returns.
    */
   public void clear() {
      modCount++;
      LinkedEntry[] tab = table;
      for (int i = 0; i < tab.length; i++)
         tab[i] = null;
      size = 0;
      header.before = header.after = header;
   }

   static class LinkedEntry<K, V> implements Map.Entry<K, V> {
      final K key;
      V value;
      LinkedEntry<K, V> next;
      final int hash;

      // These fields comprise the doubly linked list used for iteration.
      LinkedEntry<K, V> before, after;

      /**
       * Creates new entry.
       */
      LinkedEntry(int h, K k, V v, LinkedEntry<K, V> n) {
         value = v;
         next = n;
         key = k;
         hash = h;
      }

      /**
       * Removes this entry from the linked list.
       */
      private void remove() {
         before.after = after;
         after.before = before;
      }

      /**
       * Inserts this entry before the specified existing entry in the list.
       */
      private void addBefore(LinkedEntry<K, V> existingEntry) {
         after = existingEntry;
         before = existingEntry.before;
         before.after = this;
         after.before = this;
      }

      /**
       * This method is invoked by the superclass whenever the value of a pre-existing entry is read by Map.get or
       * modified by Map.set. If the enclosing Map is access-ordered, it moves the entry to the end of the list;
       * otherwise, it does nothing.
       */
      void recordAccess(BidirectionalLinkedHashMap<K, V> lm) {
         if (lm.accessOrder) {
            lm.modCount++;
            remove();
            addBefore(lm.header);
         }
      }

      void recordRemoval(BidirectionalLinkedHashMap<K, V> m) {
         remove();
      }

      public final K getKey() {
         return key;
      }

      public final V getValue() {
         return value;
      }

      public final V setValue(V newValue) {
         V oldValue = value;
         value = newValue;
         return oldValue;
      }

      public final boolean equals(Object o) {
         if (!(o instanceof Map.Entry))
            return false;
         Map.Entry e = (Map.Entry) o;
         Object k1 = getKey();
         Object k2 = e.getKey();
         if (k1 == k2 || (k1 != null && k1.equals(k2))) {
            Object v1 = getValue();
            Object v2 = e.getValue();
            if (v1 == v2 || (v1 != null && v1.equals(v2)))
               return true;
         }
         return false;
      }

      public final int hashCode() {
         return (key == null ? 0 : key.hashCode()) ^
               (value == null ? 0 : value.hashCode());
      }

      public final String toString() {
         return getKey() + "=" + getValue();
      }
   }

   // These methods are used when serializing HashSets
   int capacity() {
      return table.length;
   }

   float loadFactor() {
      return loadFactor;
   }

   /**
    * Called by superclass constructors and pseudo constructors (clone, readObject) before any entries are inserted into
    * the map.  Initializes the chain.
    */
   void init() {
      header = new LinkedEntry<K, V>(-1, null, null, null);
      header.before = header.after = header;
   }

   /**
    * Transfers all entries to new table array.  This method is called by superclass resize.  It is overridden for
    * performance, as it is faster to iterate using our linked list.
    */
   void transfer(LinkedEntry[] newTable) {
      int newCapacity = newTable.length;
      for (LinkedEntry<K, V> e = header.after; e != header; e = e.after) {
         int index = indexFor(e.hash, newCapacity);
         e.next = newTable[index];
         newTable[index] = e;
      }
   }


   /**
    * Returns <tt>true</tt> if this map maps one or more keys to the specified value.
    *
    * @param value value whose presence in this map is to be tested
    * @return <tt>true</tt> if this map maps one or more keys to the specified value
    */
   public boolean containsValue(Object value) {
      // Overridden to take advantage of faster iterator
      if (value == null) {
         for (LinkedEntry e = header.after; e != header; e = e.after)
            if (e.value == null)
               return true;
      } else {
         for (LinkedEntry e = header.after; e != header; e = e.after)
            if (value.equals(e.value))
               return true;
      }
      return false;
   }

   /**
    * Returns the value to which the specified key is mapped, or {@code null} if this map contains no mapping for the
    * key.
    * <p/>
    * <p>More formally, if this map contains a mapping from a key {@code k} to a value {@code v} such that {@code
    * (key==null ? k==null : key.equals(k))}, then this method returns {@code v}; otherwise it returns {@code null}.
    * (There can be at most one such mapping.)
    * <p/>
    * <p>A return value of {@code null} does not <i>necessarily</i> indicate that the map contains no mapping for the
    * key; it's also possible that the map explicitly maps the key to {@code null}. The {@link #containsKey containsKey}
    * operation may be used to distinguish these two cases.
    */
   public V get(Object key) {
      assertKeyNotNull(key);
      LinkedEntry<K, V> e = getEntry(key);
      if (e == null)
         return null;
      e.recordAccess(this);
      return e.value;
   }

   private abstract class LinkedHashIterator<T> implements Iterator<T> {
      LinkedEntry<K, V> nextEntry;
      LinkedEntry<K, V> lastReturned = null;
      boolean directionReversed;

      protected LinkedHashIterator(boolean directionReversed) {
         this.directionReversed = directionReversed;
         nextEntry = directionReversed ? header.before : header.after;
//         nextEntry = header;
      }

      /**
       * The modCount value that the iterator believes that the backing List should have.  If this expectation is
       * violated, the iterator has detected concurrent modification.
       */
      int expectedModCount = modCount;

      public boolean hasNext() {
         return nextEntry != header;
      }

      public void remove() {
         if (lastReturned == null)
            throw new IllegalStateException();
         if (modCount != expectedModCount)
            throw new ConcurrentModificationException();

         BidirectionalLinkedHashMap.this.remove(lastReturned.key);
         lastReturned = null;
         expectedModCount = modCount;
      }

      LinkedEntry<K, V> nextEntry() {
         if (modCount != expectedModCount)
            throw new ConcurrentModificationException();
         if (nextEntry == header)
            throw new NoSuchElementException();

         LinkedEntry<K, V> e = lastReturned = nextEntry;
         nextEntry = directionReversed ? e.before : e.after;
         return e;
      }
   }

   private class KeyIterator extends LinkedHashIterator<K> {
      protected KeyIterator(boolean directionReversed) {
         super(directionReversed);
      }

      public K next() {
         return nextEntry().getKey();
      }
   }

   private class ValueIterator extends LinkedHashIterator<V> {
      protected ValueIterator(boolean directionReversed) {
         super(directionReversed);
      }

      public V next() {
         return nextEntry().value;
      }
   }

   private class EntryIterator extends LinkedHashIterator<Map.Entry<K, V>> {
      protected EntryIterator(boolean directionReversed) {
         super(directionReversed);
      }

      public Map.Entry<K, V> next() {
         return nextEntry();
      }
   }

   /**
    * This override alters behavior of superclass put method. It causes newly allocated entry to get inserted at the end
    * of the linked list and removes the eldest entry if appropriate.
    */
   void addEntry(int hash, K key, V value, int bucketIndex) {
      createEntry(hash, key, value, bucketIndex);

      // Grow capacity if appropriate
      if (size >= threshold) resize(2 * table.length);
   }

   /**
    * This override differs from addEntry in that it doesn't resize the table or remove the eldest entry.
    */
   void createEntry(int hash, K key, V value, int bucketIndex) {
      LinkedEntry<K, V> old = table[bucketIndex];
      LinkedEntry<K, V> e = new LinkedEntry<K, V>(hash, key, value, old);
      table[bucketIndex] = e;
      e.addBefore(header);
      size++;
   }

   public Collection<V> values() {
      if (values == null) values = new Values();
      return values;
   }

   private final class Values extends AbstractCollection<V> {
      public Iterator<V> iterator() {
         return new ValueIterator(false);
      }

      public int size() {
         return BidirectionalLinkedHashMap.this.size();
      }

      public boolean contains(Object o) {
         return containsValue(o);
      }

      public void clear() {
         BidirectionalLinkedHashMap.this.clear();
      }
   }

   public ReversibleOrderedSet<K> keySet() {
      if (keySet == null) keySet = new KeySet();
      return (ReversibleOrderedSet<K>) keySet;
   }

   private class KeySet extends AbstractSet<K>
         implements ReversibleOrderedSet<K> {

      public Iterator<K> reverseIterator() {
         return new KeyIterator(true);
      }

      public Iterator<K> iterator() {
         return new KeyIterator(false);
      }

      public void clear() {
         BidirectionalLinkedHashMap.this.clear();
      }

      public boolean contains(Object o) {
         return containsKey(o);
      }

      public boolean remove(Object o) {
         int size = size();
         BidirectionalLinkedHashMap.this.remove(o);
         return size() < size;
      }

      public int size() {
         return BidirectionalLinkedHashMap.this.size();
      }
   }

   public ReversibleOrderedSet<Entry<K, V>> entrySet() {
      if (entrySet == null) entrySet = new EntrySet();
      return (ReversibleOrderedSet<Entry<K, V>>) entrySet;
   }

   private final class EntrySet extends AbstractSet<Map.Entry<K, V>>
         implements ReversibleOrderedSet<Entry<K, V>> {

      public Iterator<Map.Entry<K, V>> reverseIterator() {
         return new EntryIterator(true);
      }

      public Iterator<Map.Entry<K, V>> iterator() {
         return new EntryIterator(false);
      }

      @SuppressWarnings("unchecked")
      public boolean contains(Object o) {
         if (!(o instanceof Map.Entry))
            return false;
         Map.Entry<K, V> e = (Map.Entry<K, V>) o;
         LinkedEntry<K, V> candidate = getEntry(e.getKey());
         return candidate != null && candidate.equals(e);
      }

      public boolean remove(Object o) {
         return removeMapping(o) != null;
      }

      public int size() {
         return size;
      }

      public void clear() {
         BidirectionalLinkedHashMap.this.clear();
      }
   }

   /**
    * Returns a shallow copy of this <tt>Map</tt> instance: the keys and values themselves are not cloned.
    *
    * @return a shallow copy of this map.
    */
   @SuppressWarnings("unchecked")
   public BidirectionalLinkedHashMap clone() {

      BidirectionalLinkedHashMap<K, V> result;
      try {
         result = (BidirectionalLinkedHashMap<K, V>) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen!", e);
      }
      result.table = new LinkedEntry[table.length];
      result.entrySet = null;
      result.modCount = 0;
      result.size = 0;
      result.init();
      result.putAllForCreate(this);

      return result;
   }

   @Override
   public String toString() {
      return "BidirectionalLinkedHashMap{" +
            "size=" + size +
            '}';
   }
}
