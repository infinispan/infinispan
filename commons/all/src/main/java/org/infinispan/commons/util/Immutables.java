package org.infinispan.commons.util;

import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;


/**
 * Factory for generating immutable type wrappers.
 *
 * @author Jason T. Greene
 * @author Galder Zamarreño
 * @author Tristan Tarrant
 * @since 4.0
 */
public class Immutables {
   /**
    * Converts a Collection to an immutable List by copying it.
    *
    * @param source the collection to convert
    * @return a copied/converted immutable list
    */
   public static <T> List<T> immutableListConvert(Collection<? extends T> source) {
      return new ImmutableListCopy<>(source);
   }

   /**
    * Creates an immutable copy of the properties.
    *
    * @param properties the TypedProperties to copy
    * @return the immutable copy
    */
   public static TypedProperties immutableTypedProperties(TypedProperties properties) {
      if (properties == null) return null;
      return new ImmutableTypedProperties(properties);
   }

   /**
    * Wraps an array with an immutable list. There is no copying involved.
    *
    * @param array the array to wrap
    * @return a list containing the array
    */
   public static <T> List<T> immutableListWrap(T... array) {
      return new ImmutableListCopy<>(array);
   }

   public static <T> ImmutableListCopy<T> immutableListAdd(List<T> list, int position, T element) {
      T[] copy = (T[]) new Object[list.size() + 1];
      for (int i = 0; i < position; i++) {
         copy[i] = list.get(i);
      }
      copy[position] = element;
      for (int i = position; i < list.size(); i++) {
         copy[i + 1] = list.get(i);
      }
      return new ImmutableListCopy<>(copy);
   }

   public static <T> ImmutableListCopy<T> immutableListReplace(List<T> list, int position, T element) {
      T[] copy = (T[]) new Object[list.size()];
      for (int i = 0; i < position; i++) {
         copy[i] = list.get(i);
      }
      copy[position] = element;
      for (int i = position + 1; i < list.size(); i++) {
         copy[i] = list.get(i);
      }
      return new ImmutableListCopy<>(copy);
   }

   public static <T> List<T> immutableListRemove(List<T> list, int position) {
      T[] copy = (T[]) new Object[list.size() - 1];
      for (int i = 0; i < position; i++) {
         copy[i] = list.get(i);
      }
      for (int i = position + 1; i < list.size(); i++) {
         copy[i - 1] = list.get(i);
      }
      return new ImmutableListCopy<>(copy);
   }

   /**
    * Wraps a set with an immutable set. There is no copying involved.
    *
    * @param set the set to wrap
    * @return an immutable set wrapper that delegates to the original set
    */
   public static <T> Set<T> immutableSetWrap(Set<? extends T> set) {
      return new ImmutableSetWrapper<>(set);
   }

   /**
    * Wraps a map with an immutable map. There is no copying involved.
    *
    * @param map the map to wrap
    * @return an immutable map wrapper that delegates to the original map
    */
   public static <K, V> Map<K, V> immutableMapWrap(Map<? extends K, ? extends V> map) {
      return new ImmutableMapWrapper<>(map);
   }

   public interface  Immutable {
   }

   /*
    * Immutable wrapper types.
    *
    * We have to re-implement Collections.unmodifiableXXX, since it is not
    * simple to detect them (the class names are JDK dependent).
    */
   public static class ImmutableIteratorWrapper<E> implements Iterator<E> {
      private final Iterator<? extends E> iterator;

      public ImmutableIteratorWrapper(Iterator<? extends E> iterator) {
         this.iterator = iterator;
      }

      @Override
      public boolean hasNext() {
         return iterator.hasNext();
      }

      @Override
      public E next() {
         return iterator.next();
      }

      // Use the default remove() implementation

      @Override
      public void forEachRemaining(Consumer<? super E> action) {
         iterator.forEachRemaining(action);
      }
   }

   private static class ImmutableCollectionWrapper<E> implements Collection<E>, Serializable, Immutable {
      private static final long serialVersionUID = 6777564328198393535L;

      Collection<? extends E> collection;

      public ImmutableCollectionWrapper(Collection<? extends E> collection) {
         this.collection = collection;
      }

      @Override
      public boolean add(E o) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean addAll(Collection<? extends E> c) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean contains(Object o) {
         return collection.contains(o);
      }

      @Override
      public boolean containsAll(Collection<?> c) {
         return collection.containsAll(c);
      }

      @Override
      public boolean equals(Object o) {
         return collection.equals(o);
      }

      @Override
      public int hashCode() {
         return collection.hashCode();
      }

      @Override
      public boolean isEmpty() {
         return collection.isEmpty();
      }

      @Override
      public Iterator<E> iterator() {
         return new ImmutableIteratorWrapper<>(collection.iterator());
      }

      @Override
      public boolean remove(Object o) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean removeAll(Collection<?> c) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean retainAll(Collection<?> c) {
         throw new UnsupportedOperationException();
      }

      @Override
      public int size() {
         return collection.size();
      }

      @Override
      public Object[] toArray() {
         return collection.toArray();
      }

      @Override
      public <T> T[] toArray(T[] a) {
         return collection.toArray(a);
      }

      @Override
      public String toString() {
         return collection.toString();
      }
   }

   /**
    * Immutable version of Map.Entry for traversing immutable collections.
    */
   private static class ImmutableEntry<K, V> implements Entry<K, V>, Immutable {
      private final K key;
      private final V value;
      private final int hash;

      ImmutableEntry(Entry<? extends K, ? extends V> entry) {
         this.key = entry.getKey();
         this.value = entry.getValue();
         this.hash = entry.hashCode();
      }

      ImmutableEntry(K key, V value) {
         this.key = key;
         this.value = value;
         this.hash = Objects.hashCode(key) ^ Objects.hashCode(value);
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
         throw new UnsupportedOperationException();
      }

      private static boolean eq(Object o1, Object o2) {
         return Objects.equals(o1, o2);
      }

      @Override
      @SuppressWarnings("unchecked")
      public boolean equals(Object o) {
         if (!(o instanceof Entry))
            return false;

         Entry<K, V> entry = (Entry<K, V>) o;
         return eq(entry.getKey(), key) && eq(entry.getValue(), value);
      }

      @Override
      public int hashCode() {
         return hash;
      }

      @Override
      public String toString() {
         return getKey() + "=" + getValue();
      }
   }

   private static class ImmutableSetWrapper<E> extends ImmutableCollectionWrapper<E> implements Set<E>, Serializable, Immutable {
      private static final long serialVersionUID = 7991492805176142615L;

      public ImmutableSetWrapper(Set<? extends E> set) {
         super(set);
      }
   }

   private static class ImmutableEntrySetWrapper<K, V> extends ImmutableSetWrapper<Entry<K, V>> {
      private static final long serialVersionUID = 6378667653889667692L;

      @SuppressWarnings("unchecked")
      public ImmutableEntrySetWrapper(Set<? extends Entry<? extends K, ? extends V>> set) {
         super((Set<Entry<K, V>>) set);
      }

      @Override
      public Object[] toArray() {
         Object[] array = new Object[collection.size()];
         int i = 0;
         for (Entry<K, V> entry : this)
            array[i++] = entry;
         return array;
      }

      @Override
      @SuppressWarnings("unchecked")
      public <T> T[] toArray(T[] array) {
         int size = collection.size();
         if (array.length < size)
            array = (T[]) Array.newInstance(array.getClass().getComponentType(), size);

         int i = 0;
         Object[] result = array;
         for (Entry<K, V> entry : this)
            result[i++] = entry;

         return array;
      }

      @Override
      public Iterator<Entry<K, V>> iterator() {
         return new ImmutableIteratorWrapper<Entry<K, V>>(collection.iterator()) {
            @Override
            public Entry<K, V> next() {
               return new ImmutableEntry<>(super.next());
            }
         };
      }
   }

   public static class ImmutableMapWrapper<K, V> implements Map<K, V>, Serializable, Immutable {
      private static final long serialVersionUID = 708144227046742221L;

      private final Map<? extends K, ? extends V> map;

      public ImmutableMapWrapper(Map<? extends K, ? extends V> map) {
         this.map = map;
      }

      @Override
      public void clear() {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean containsKey(Object key) {
         return map.containsKey(key);
      }

      @Override
      public boolean containsValue(Object value) {
         return map.containsValue(value);
      }

      @Override
      public Set<Entry<K, V>> entrySet() {
         return new ImmutableEntrySetWrapper<>(map.entrySet());
      }

      @Override
      public boolean equals(Object o) {
         return map.equals(o);
      }

      @Override
      public V get(Object key) {
         return map.get(key);
      }

      @Override
      public int hashCode() {
         return map.hashCode();
      }

      @Override
      public boolean isEmpty() {
         return map.isEmpty();
      }

      @Override
      public Set<K> keySet() {
         return new ImmutableSetWrapper<>(map.keySet());
      }

      @Override
      public V put(K key, V value) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void putAll(Map<? extends K, ? extends V> t) {
         throw new UnsupportedOperationException();
      }

      @Override
      public V remove(Object key) {
         throw new UnsupportedOperationException();
      }

      @Override
      public int size() {
         return map.size();
      }

      @Override
      public Collection<V> values() {
         return new ImmutableCollectionWrapper<>(map.values());
      }

      @Override
      public String toString() {
         return map.toString();
      }
   }

   private static class ImmutableTypedProperties extends TypedProperties {

      ImmutableTypedProperties(TypedProperties properties) {
         super();
         if (properties != null && !properties.isEmpty()) {
            for (Map.Entry<Object, Object> e: properties.entrySet()) super.put(e.getKey(), e.getValue());
         }
      }

      @Override
      public synchronized void clear() {
         throw new UnsupportedOperationException();
      }

      @Override
      public Set<java.util.Map.Entry<Object, Object>> entrySet() {
         return new ImmutableEntrySetWrapper<>(super.entrySet());
      }

      @Override
      public Set<Object> keySet() {
         return new ImmutableSetWrapper<>(super.keySet());
      }

      @Override
      public synchronized void load(InputStream inStream) {
         throw new UnsupportedOperationException();
      }

      @Override
      public synchronized void load(Reader reader) {
         throw new UnsupportedOperationException();
      }

      @Override
      public synchronized void loadFromXML(InputStream in) {
         throw new UnsupportedOperationException();
      }

      @Override
      public synchronized Object put(Object key, Object value) {
         throw new UnsupportedOperationException();
      }

      @Override
      public synchronized void putAll(Map<?, ?> t) {
         throw new UnsupportedOperationException();
      }

      @Override
      public synchronized Object remove(Object key) {
         throw new UnsupportedOperationException();
      }

      @Override
      public synchronized TypedProperties setProperty(String key, String value) {
         throw new UnsupportedOperationException();
      }

      @Override
      public Set<String> stringPropertyNames() {
         return new ImmutableSetWrapper<>(super.stringPropertyNames());
      }

      @Override
      public Collection<Object> values() {
         return new ImmutableCollectionWrapper<>(super.values());
      }

   }


}
