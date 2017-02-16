package org.infinispan.commons.util;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.AbstractSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * This Map will remove entries when the value in the map has been cleaned from
 * garbage collection
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author <a href="mailto:bill@jboss.org">Bill Burke</a>
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 * @see <a href="http://anonsvn.jboss.org/repos/common/common-core/trunk/src/main/java/org/jboss/util/collection/">
 *    JBoss Common Core source code for origins of this class</a>
 */
public final class WeakValueHashMap<K, V> extends java.util.AbstractMap<K, V> {

   /**
    * Hash table mapping keys to ref values
    */
   private Map<K, ValueRef<K, V>> map;

   /**
    * Reference queue for cleared RefKeys
    */
   private ReferenceQueue<V> queue = new ReferenceQueue<V>();

   /**
    * Constructs a new, empty <code>WeakValueHashMap</code> with the given
    * initial capacity and the given load factor.
    *
    * @param initialCapacity The initial capacity of the <code>WeakValueHashMap</code>
    * @param loadFactor      The load factor of the <code>WeakValueHashMap</code>
    * @throws IllegalArgumentException If the initial capacity is less than
    *                                  zero, or if the load factor is
    *                                  nonpositive
    */
   public WeakValueHashMap(int initialCapacity, float loadFactor) {
      map = createMap(initialCapacity, loadFactor);
   }

   /**
    * Constructs a new, empty <code>WeakValueHashMap</code> with the given
    * initial capacity and the default load factor, which is
    * <code>0.75</code>.
    *
    * @param initialCapacity The initial capacity of the <code>WeakValueHashMap</code>
    * @throws IllegalArgumentException If the initial capacity is less than
    *                                  zero
    */
   public WeakValueHashMap(int initialCapacity) {
      map = createMap(initialCapacity);
   }

   /**
    * Constructs a new, empty <code>WeakValueHashMap</code> with the default
    * initial capacity and the default load factor, which is
    * <code>0.75</code>.
    */
   public WeakValueHashMap() {
      map = createMap();
   }

   /**
    * Constructs a new <code>WeakValueHashMap</code> with the same mappings as
    * the specified <tt>Map</tt>.  The <code>WeakValueHashMap</code> is created
    * with an initial capacity of twice the number of mappings in the specified
    * map or 11 (whichever is greater), and a default load factor, which is
    * <tt>0.75</tt>.
    *
    * @param t the map whose mappings are to be placed in this map.
    * @since 1.3
    */
   public WeakValueHashMap(Map<K, V> t) {
      this(Math.max(2 * t.size(), 11), 0.75f);
      putAll(t);
   }

   /**
    * Create new value ref instance.
    *
    * @param key   the key
    * @param value the value
    * @param q     the ref queue
    * @return new value ref instance
    */
   private ValueRef<K, V> create(K key, V value, ReferenceQueue<V> q) {
      return WeakValueRef.create(key, value, q);
   }

   /**
    * Create map.
    *
    * @param initialCapacity the initial capacity
    * @param loadFactor      the load factor
    * @return new map instance
    */
   private Map<K, ValueRef<K, V>> createMap(int initialCapacity, float loadFactor) {
      return new HashMap<K, ValueRef<K, V>>(initialCapacity, loadFactor);
   }

   /**
    * Create map.
    *
    * @param initialCapacity the initial capacity
    * @return new map instance
    */
   private Map<K, ValueRef<K, V>> createMap(int initialCapacity) {
      return new HashMap<K, ValueRef<K, V>>(initialCapacity);
   }

   /**
    * Create map.
    *
    * @return new map instance
    */
   protected Map<K, ValueRef<K, V>> createMap() {
      return new HashMap<K, ValueRef<K, V>>();
   }

   @Override
   public int size() {
      processQueue();
      return map.size();
   }

   @Override
   public boolean containsKey(Object key) {
      processQueue();
      return map.containsKey(key);
   }

   @Override
   public V get(Object key) {
      processQueue();
      ValueRef<K, V> ref = map.get(key);
      if (ref != null)
         return ref.get();
      return null;
   }

   @Override
   public V put(K key, V value) {
      processQueue();
      ValueRef<K, V> ref = create(key, value, queue);
      ValueRef<K, V> result = map.put(key, ref);
      if (result != null)
         return result.get();
      return null;
   }

   @Override
   public V remove(Object key) {
      processQueue();
      ValueRef<K, V> result = map.remove(key);
      if (result != null)
         return result.get();
      return null;
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      processQueue();
      return new EntrySet();
   }

   @Override
   public void clear() {
      processQueue();
      map.clear();
   }

   @Override
   public String toString() {
      return map.toString();
   }

   /**
    * Remove all entries whose values have been discarded.
    */
   @SuppressWarnings("unchecked")
   private void processQueue() {
      ValueRef<K, V> ref = (ValueRef<K, V>) queue.poll();
      while (ref != null) {
         // only remove if it is the *exact* same WeakValueRef
         if (ref == map.get(ref.getKey()))
            map.remove(ref.getKey());

         ref = (ValueRef<K, V>) queue.poll();
      }
   }

   /**
    * EntrySet.
    */
   private class EntrySet extends AbstractSet<Entry<K, V>> {

      @Override
      public Iterator<Entry<K, V>> iterator() {
         return new EntrySetIterator(map.entrySet().iterator());
      }

      @Override
      public int size() {
         return WeakValueHashMap.this.size();
      }
   }

   /**
    * EntrySet iterator
    */
   private class EntrySetIterator implements Iterator<Entry<K, V>> {

      /**
       * The delegate
       */
      private Iterator<Entry<K, ValueRef<K, V>>> delegate;

      /**
       * Create a new EntrySetIterator.
       *
       * @param delegate the delegate
       */
      public EntrySetIterator(Iterator<Entry<K, ValueRef<K, V>>> delegate) {
         this.delegate = delegate;
      }

      public boolean hasNext() {
         return delegate.hasNext();
      }

      public Entry<K, V> next() {
         Entry<K, ValueRef<K, V>> next = delegate.next();
         return next.getValue();
      }

      public void remove() {
         throw new UnsupportedOperationException("remove");
      }
   }

   /**
    * Weak value ref.
    *
    * @param <K> the key type
    * @param <V> the value type
    * @author <a href="mailto:bill@jboss.org">Bill Burke</a>
    * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
    * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
    */
   private static class WeakValueRef<K, V>
         extends WeakReference<V> implements ValueRef<K, V> {

      /**
       * The key
       */
      public K key;

      /**
       * Safely create a new WeakValueRef
       *
       * @param <K> the key type
       * @param <V> the value type
       * @param key the key
       * @param val the value
       * @param q   the reference queue
       * @return the reference or null if the value is null
       */
      static <K, V> WeakValueRef<K, V> create(K key, V val, ReferenceQueue<V> q) {
         if (val == null)
            return null;
         else
            return new WeakValueRef<K, V>(key, val, q);
      }

      /**
       * Create a new WeakValueRef.
       *
       * @param key the key
       * @param val the value
       * @param q   the reference queue
       */
      private WeakValueRef(K key, V val, ReferenceQueue<V> q) {
         super(val, q);
         this.key = key;
      }

      public K getKey() {
         return key;
      }

      public V getValue() {
         return get();
      }

      public V setValue(V value) {
         throw new UnsupportedOperationException("setValue");
      }

      @Override
      public String toString() {
         return String.valueOf(get());
      }
   }

   public interface ValueRef<K, V> extends Map.Entry<K, V> {

      /**
       * Get underlying value.
       *
       * @return the value
       */
      V get();
   }

}
