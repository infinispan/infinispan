package org.infinispan.commons.equivalence;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Custom hash-based linked list map which accepts no null keys nor null values,
 * where equality and hash code calculations are done based on passed
 * {@link org.infinispan.commons.equivalence.Equivalence} function implementations for keys
 * and values, as opposed to relying on their own equals/hashCode/toString
 * implementations. This is handy when using key/values whose mentioned
 * methods cannot be overriden, i.e. arrays, and in situations where users
 * want to avoid using wrapper objects.
 *
 * In order to provide linked list behaviour, entries are linked with each
 * other in a predictable order.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public class EquivalentLinkedHashMap<K, V> extends EquivalentHashMap<K, V> {

   private transient LinkedNode<K, V> header;

   private final IterationOrder iterationOrder;

   public EquivalentLinkedHashMap(int initialCapacity, float loadFactor,
         IterationOrder iterationOrder,
         Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
      super(initialCapacity, loadFactor, keyEq, valueEq);
      this.iterationOrder = iterationOrder;
      addFirstEntry();
   }

   private void addFirstEntry() {
      header = createLinkedNode();
      header.before = header;
      header.after = header;
   }

   @Override
   void addEntry(int index, K key, V value, int hash) {
      super.addEntry(index, key, value, hash);
      LinkedNode<K,V> eldest = header.after;
      if (removeEldestEntry(eldest)) {
         remove(eldest.getKey());
      }
   }

   protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return false;
   }

   @SuppressWarnings("unchecked")
   private <K, V> LinkedNode<K, V> createLinkedNode() {
      return new LinkedNode<K, V>(null, -1, null, null);
   }

   @Override
   Node<K, V> createNode(K key, V value, int hash, Node<K, V> node) {
      LinkedNode<K, V> linkedNode = new LinkedNode<K, V>(key, hash, value, node);
      linkedNode.addBefore(header);
      return linkedNode;
   }

   @Override
   public V get(Object key) {
      LinkedNode<K, V> n = getNode(key);
      return n == null ? null : n.recordAccess(this);
   }

   @Override
   public V remove(Object key) {
      LinkedNode<K, V> prevNode = removeNode(key);
      return prevNode == null ? null : prevNode.remove();
   }

   @Override
   public void clear() {
      super.clear();
      header.before = header;
      header.after = header;
   }

   private static final class LinkedNode<K, V> extends Node<K,V> {

      LinkedNode<K, V> before;
      LinkedNode<K, V> after;

      private LinkedNode(K key, int hash, V value, Node<K, V> next) {
         super(key, hash, value, next);
      }

      private V remove() {
         before.after = after;
         after.before = before;
         return value;
      }

      private void addBefore(LinkedNode<K, V> entry) {
         after  = entry;
         before = entry.before;
         before.after = this;
         after.before = this;
      }

      V recordAccess(EquivalentHashMap<K, V> m) {
         EquivalentLinkedHashMap<K, V> linkedMap = (EquivalentLinkedHashMap<K, V>)m;
         if (linkedMap.iterationOrder == IterationOrder.ACCESS_ORDER) {
            linkedMap.modCount++;
            remove();
            addBefore(linkedMap.header);
         }
         return value;
      }

      @Override
      protected V setValue(V value, EquivalentHashMap<K, V> map) {
         V retValue = super.setValue(value, map);
         recordAccess(map);
         return retValue;
      }
   }

   public enum IterationOrder {
      ACCESS_ORDER, INSERT_ORDER;

      public boolean toJdkAccessOrder() {
         return this == ACCESS_ORDER;
      }
   }

   /**
    * Exported Entry for iterators
    */
   private static class LinkedEntry<K,V> extends MapEntry<K,V> {
      LinkedNode<K, V> before;
      LinkedNode<K, V> after;

      LinkedEntry(K key, V val, LinkedNode<K, V> before, LinkedNode<K, V> after, EquivalentHashMap<K, V> map) {
         super(key, val, map);
         this.before = before;
         this.after = after;
      }
   }

   private abstract class EquivalentLinkedHashIterator<T> implements Iterator<T> {
      final EquivalentHashMap<K, V> map;
      LinkedEntry<K, V> nextEntry;
      LinkedEntry<K, V> lastReturned = null;

      protected EquivalentLinkedHashIterator(EquivalentHashMap<K, V> map) {
         this.map = map;
         nextEntry = new LinkedEntry<K, V>(
               header.after.key, header.after.value,
               header.after.before, header.after.after, map);
      }

      /**
       * The modCount value that the iterator believes that the backing
       * List should have.  If this expectation is violated, the iterator
       * has detected concurrent modification.
       */
      int expectedModCount = modCount;

      public boolean hasNext() {
         return !equals(nextEntry, header);
      }

      public void remove() {
         if (lastReturned == null)
            throw new IllegalStateException();
         if (modCount != expectedModCount)
            throw new ConcurrentModificationException();

         EquivalentLinkedHashMap.this.remove(lastReturned.key);
         lastReturned = null;
         expectedModCount = modCount;
      }

      LinkedEntry<K,V> nextEntry() {
         if (modCount != expectedModCount)
            throw new ConcurrentModificationException();
         if (equals(nextEntry, header))
            throw new NoSuchElementException();

         LinkedEntry<K, V> e = nextEntry;
         lastReturned = nextEntry;
         nextEntry = new LinkedEntry<K, V>(
               e.after.key, e.after.value,
               e.after.before, e.after.after, map);
         return e;
      }

      boolean equals(LinkedEntry<K, V> entry, LinkedNode<K, V> node) {
         return entry.key == node.key
               && entry.val == node.value
               && entry.before == node.before
               && entry.after == node.after;
      }
   }

   private class KeyIterator extends EquivalentLinkedHashIterator<K> {

      protected KeyIterator(EquivalentHashMap<K, V> map) {
         super(map);
      }

      public K next() {
         return nextEntry().getKey();
      }
   }

   private class ValueIterator extends EquivalentLinkedHashIterator<V> {

      protected ValueIterator(EquivalentHashMap<K, V> map) {
         super(map);
      }

      public V next() {
         return nextEntry().val;
      }
   }

   private class EntryIterator extends EquivalentLinkedHashIterator<Map.Entry<K, V>> {

      protected EntryIterator(EquivalentHashMap<K, V> map) {
         super(map);
      }

      public Map.Entry<K, V> next() {
         return nextEntry();
      }
   }

   Iterator<K> newKeyIterator() {
      return new KeyIterator(this);
   }

   Iterator<V> newValueIterator() {
      return new ValueIterator(this);
   }

   Iterator<Map.Entry<K,V>> newEntryIterator() {
      return new EntryIterator(this);
   }

}
