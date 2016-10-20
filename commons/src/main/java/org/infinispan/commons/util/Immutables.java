package org.infinispan.commons.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Reader;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.MarshallUtil;


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
    * Whether or not this collection type is immutable
    *
    * @param o a Collection, Set, List, or Map
    * @return true if immutable, false if not
    */
   public static boolean isImmutable(Object o) {
      return o instanceof Immutable;
   }

   /**
    * Converts a Collection to an immutable List by copying it.
    *
    * @param source the collection to convert
    * @return a copied/converted immutable list
    */
   public static <T> List<T> immutableListConvert(Collection<? extends T> source) {
      return new ImmutableListCopy<T>(source);
   }

   /**
    *
    * Creates an immutable copy of the list.
    *
    * @param list the list to copy
    * @return the immutable copy
    */
   public static <T> List<T> immutableListCopy(List<T> list) {
      if (list == null) return null;
      if (list.isEmpty()) return Collections.emptyList();
      if (list.size() == 1) return Collections.singletonList(list.get(0));
      return new ImmutableListCopy<T>(list);
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
    * @param <T>
    * @param array the array to wrap
    * @return a list containing the array
    */
   public static <T> List<T> immutableListWrap(T... array) {
      return new ImmutableListCopy<T>(array);
   }

   /**
    * Creates a new immutable list containing the union (combined entries) of both lists.
    *
    * @param list1 contains the first elements of the new list
    * @param list2 contains the successor elements of the new list
    * @return a new immutable merged copy of list1 and list2
    */
   public static <T> List<T> immutableListMerge(List<? extends T> list1, List<? extends T> list2) {
      return new ImmutableListCopy<T>(list1, list2);
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
      return new ImmutableListCopy<>((T[]) copy);
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
    * Converts a Collections into an immutable Set by copying it.
    *
    * @param collection the collection to convert/copy
    * @return a new immutable set containing the elements in collection
    */
   public static <T> Set<T> immutableSetConvert(Collection<? extends T> collection) {
      return immutableSetWrap(new HashSet<T>(collection));
   }

   /**
    * Wraps a set with an immutable set. There is no copying involved.
    *
    * @param set the set to wrap
    * @return an immutable set wrapper that delegates to the original set
    */
   public static <T> Set<T> immutableSetWrap(Set<? extends T> set) {
      return new ImmutableSetWrapper<T>(set);
   }

   /**
    * Creates an immutable copy of the specified set.
    *
    * @param set the set to copy from
    * @return an immutable set copy
    */
   public static <T> Set<T> immutableSetCopy(Set<T> set) {
      if (set == null) return null;
      if (set.isEmpty()) return Collections.emptySet();
      if (set.size() == 1) return Collections.singleton(set.iterator().next());
      Set<? extends T> copy = ObjectDuplicator.duplicateSet(set);
      if (copy == null)
         // Set uses Collection copy-ctor
         copy = attemptCopyConstructor(set, Collection.class);
      if (copy == null)
         copy = new HashSet<T>(set);

      return new ImmutableSetWrapper<T>(copy);
   }


   /**
    * Wraps a map with an immutable map. There is no copying involved.
    *
    * @param map the map to wrap
    * @return an immutable map wrapper that delegates to the original map
    */
   public static <K, V> Map<K, V> immutableMapWrap(Map<? extends K, ? extends V> map) {
      return new ImmutableMapWrapper<K, V>(map);
   }

   /**
    * Creates an immutable copy of the specified map.
    *
    * @param map the map to copy from
    * @return an immutable map copy
    */
   public static <K, V> Map<K, V> immutableMapCopy(Map<K, V> map) {
      if (map == null) return null;
      if (map.isEmpty()) return Collections.emptyMap();
      if (map.size() == 1) {
         Map.Entry<K, V> me = map.entrySet().iterator().next();
         return Collections.singletonMap(me.getKey(), me.getValue());
      }

      Map<? extends K, ? extends V> copy = ObjectDuplicator.duplicateMap(map);

      if (copy == null)
         copy = attemptCopyConstructor(map, Map.class);
      if (copy == null)
         copy = new HashMap<K, V>(map);

      return new ImmutableMapWrapper<K, V>(copy);
   }

   /**
    * Creates a new immutable copy of the specified Collection.
    *
    * @param collection the collection to copy
    * @return an immutable copy
    */
   public static <T> Collection<T> immutableCollectionCopy(Collection<T> collection) {
      if (collection == null) return null;
      if (collection.isEmpty()) return Collections.emptySet();
      if (collection.size() == 1) return Collections.singleton(collection.iterator().next());

      Collection<? extends T> copy = ObjectDuplicator.duplicateCollection(collection);
      if (copy == null)
         copy = attemptCopyConstructor(collection, Collection.class);
      if (copy == null)
         copy = new ArrayList<T>(collection);

      return new ImmutableCollectionWrapper<T>(copy);
   }

   /**
    * Wraps a collection with an immutable collection. There is no copying involved.
    *
    * @param collection the collection to wrap
    * @return an immutable collection wrapper that delegates to the original collection
    */
   public static <T> Collection<T> immutableCollectionWrap(Collection<? extends T> collection) {
      return new ImmutableCollectionWrapper<T>(collection);
   }

   @SuppressWarnings("unchecked")
   private static <T> T attemptCopyConstructor(T source, Class<? super T> clazz) {
      try {
         return (T) source.getClass().getConstructor(clazz).newInstance(source);
      }
      catch (Exception e) {
      }

      return null;
   }

   /**
    * Wraps a {@link Map.Entry}} with an immutable {@link Map.Entry}}. There is no copying involved.
    *
    * @param entry the mapping to wrap.
    * @return an immutable {@link Map.Entry}} wrapper that delegates to the original mapping.
    */
   public static <K, V> Map.Entry<K, V> immutableEntry(Map.Entry<K, V> entry) {
      return new ImmutableEntry<K, V>(entry);
   }

   /**
    * Wraps a key and value with an immutable {@link Map.Entry}}. There is no copying involved.
    *
    * @param key the key to wrap.
    * @param value the value to wrap.
    * @return an immutable {@link Map.Entry}} wrapper that delegates to the original mapping.
    */
   public static <K, V> Map.Entry<K, V> immutableEntry(K key, V value) {
      return new ImmutableEntry<K, V>(key, value);
   }

   public interface  Immutable {
   }

   /*
    * Immutable wrapper types.
    *
    * We have to re-implement Collections.unmodifiableXXX, since it is not
    * simple to detect them (the class names are JDK dependent).
    */

   private static class ImmutableIteratorWrapper<E> implements Iterator<E> {
      private Iterator<? extends E> iterator;

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

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
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
         return new ImmutableIteratorWrapper<E>(collection.iterator());
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
      private K key;
      private V value;
      private int hash;

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
         return o1 == o2 || (o1 != null && o1.equals(o2));
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

   public static class ImmutableEntryExternalizer extends AbstractExternalizer<ImmutableEntry> {
      @Override
      public Set<Class<? extends ImmutableEntry>> getTypeClasses() {
         return Util.<Class<? extends ImmutableEntry>>asSet(ImmutableEntry.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ImmutableEntry object) throws IOException {
         output.writeObject(object.key);
         output.writeObject(object.value);
      }

      @Override
      public ImmutableEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object key = input.readObject();
         Object value = input.readObject();
         return new ImmutableEntry(key, value);
      }

      @Override
      public Integer getId() {
         return Ids.IMMUTABLE_ENTRY;
      }
   }

   private static class ImmutableSetWrapper<E> extends ImmutableCollectionWrapper<E> implements Set<E>, Serializable, Immutable {
      private static final long serialVersionUID = 7991492805176142615L;

      public ImmutableSetWrapper(Set<? extends E> set) {
         super(set);
      }
   }

   private static class ImmutableReversibleOrderedSetWrapper<E> extends ImmutableCollectionWrapper<E> implements ReversibleOrderedSet<E>, Serializable, Immutable {
      private static final long serialVersionUID = 7991492805176142615L;

      public ImmutableReversibleOrderedSetWrapper(Set<? extends E> set) {
         super(set);
      }

      @Override
      public Iterator<E> reverseIterator() {
         return new ImmutableIteratorWrapper<E>(((ReversibleOrderedSet<? extends E>) collection).reverseIterator());
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
               return new ImmutableEntry<K, V>(super.next());
            }
         };
      }
   }

   public static class ImmutableSetWrapperExternalizer extends AbstractExternalizer<Set> {
      @Override
      public void writeObject(ObjectOutput output, Set set) throws IOException {
         MarshallUtil.marshallCollection(set, output);
      }

      @Override
      public Set readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Set<Object> set = MarshallUtil.unmarshallCollection(input, HashSet::new);
         return Immutables.immutableSetWrap(set);
      }

      @Override
      public Integer getId() {
         return Ids.IMMUTABLE_SET;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends Set>> getTypeClasses() {
         return Util.<Class<? extends Set>>asSet(ImmutableSetWrapper.class);
      }
   }

   private static class ImmutableMapWrapper<K, V> implements Map<K, V>, Serializable, Immutable {
      private static final long serialVersionUID = 708144227046742221L;

      private Map<? extends K, ? extends V> map;

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
         return new ImmutableEntrySetWrapper<K, V>(map.entrySet());
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
         return new ImmutableSetWrapper<K>(map.keySet());
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
         return new ImmutableCollectionWrapper<V>(map.values());
      }

      @Override
      public String toString() {
         return map.toString();
      }
   }

   public static class ImmutableMapWrapperExternalizer extends AbstractExternalizer<Map> {
      @Override
      public void writeObject(ObjectOutput output, Map map) throws IOException {
         MarshallUtil.marshallMap(map, output);
      }

      @Override
      public Map readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return Immutables.immutableMapWrap(MarshallUtil.unmarshallMap(input, HashMap::new));
      }

      @Override
      public Integer getId() {
         return Ids.IMMUTABLE_MAP;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends Map>> getTypeClasses() {
         return Util.<Class<? extends Map>>asSet(ImmutableMapWrapper.class);
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
         return new ImmutableEntrySetWrapper<Object, Object>(super.entrySet());
      }

      @Override
      public Set<Object> keySet() {
         return new ImmutableSetWrapper<Object>(super.keySet());
      }

      @Override
      public synchronized void load(InputStream inStream) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public synchronized void load(Reader reader) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public synchronized void loadFromXML(InputStream in) throws IOException, InvalidPropertiesFormatException {
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
         return new ImmutableSetWrapper<String>(super.stringPropertyNames());
      }

      @Override
      public Collection<Object> values() {
         return new ImmutableCollectionWrapper<Object>(super.values());
      }

   }


}
