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

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.MarshallUtil;

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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * Factory for generating immutable type wrappers.
 *
 * @author Jason T. Greene
 * @author Galder Zamarreño
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
   public static <T> List<T> immutableListCopy(List<? extends T> list) {
      if (list == null) return null;
      if (list.isEmpty()) return Collections.emptyList();
      if (list.size() == 1) return Collections.<T>singletonList(list.get(0));
      return new ImmutableListCopy<T>(list);
   }

   /**
    * Creates an immutable copy of the properties.
    *
    * @param properties the TypedProperties to copy
    * @return the immutable copy
    */
   public static TypedProperties immutableTypedPropreties(TypedProperties properties) {
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
   public static <T> Set<T> immutableSetCopy(Set<? extends T> set) {
      if (set == null) return null;
      if (set.isEmpty()) return Collections.emptySet();
      if (set.size() == 1) return Collections.<T>singleton(set.iterator().next());
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
   public static <K, V> Map<K, V> immutableMapCopy(Map<? extends K, ? extends V> map) {
      if (map == null) return null;
      if (map.isEmpty()) return Collections.emptyMap();
      if (map.size() == 1) {
         Map.Entry<? extends K, ? extends V> me = map.entrySet().iterator().next();
         return Collections.<K,V>singletonMap(me.getKey(), me.getValue());
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
   public static <T> Collection<T> immutableCollectionCopy(Collection<? extends T> collection) {
      if (collection == null) return null;
      if (collection.isEmpty()) return Collections.emptySet();
      if (collection.size() == 1) return Collections.<T>singleton(collection.iterator().next());

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

   public static <T> ReversibleOrderedSet<T> immutableReversibleOrderedSetCopy(ReversibleOrderedSet<T> set) {
      Set<? extends T> copy = ObjectDuplicator.duplicateSet(set);
      if (copy == null)
         // Set uses Collection copy-ctor
         copy = attemptCopyConstructor(set, ReversibleOrderedSet.class);
      if (copy == null)
         copy = new VisitableBidirectionalLinkedHashSet<T>(false, set);

      return new ImmutableReversibleOrderedSetWrapper<T>(copy);
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
    * Wraps a {@link InternalCacheEntry}} with an immutable {@link InternalCacheEntry}}. There is no copying involved.
    *
    * @param entry the internal cache entry to wrap.
    * @return an immutable {@link InternalCacheEntry}} wrapper that delegates to the original entry.
    */
   public static InternalCacheEntry immutableInternalCacheEntry(InternalCacheEntry entry) {
      return new ImmutableInternalCacheEntry(entry);
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

      public boolean hasNext() {
         return iterator.hasNext();
      }

      public E next() {
         return iterator.next();
      }

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

      public boolean add(E o) {
         throw new UnsupportedOperationException();
      }

      public boolean addAll(Collection<? extends E> c) {
         throw new UnsupportedOperationException();
      }

      public void clear() {
         throw new UnsupportedOperationException();
      }

      public boolean contains(Object o) {
         return collection.contains(o);
      }

      public boolean containsAll(Collection<?> c) {
         return collection.containsAll(c);
      }

      public boolean equals(Object o) {
         return collection.equals(o);
      }

      public int hashCode() {
         return collection.hashCode();
      }

      public boolean isEmpty() {
         return collection.isEmpty();
      }

      public Iterator<E> iterator() {
         return new ImmutableIteratorWrapper<E>(collection.iterator());
      }

      public boolean remove(Object o) {
         throw new UnsupportedOperationException();
      }

      public boolean removeAll(Collection<?> c) {
         throw new UnsupportedOperationException();
      }

      public boolean retainAll(Collection<?> c) {
         throw new UnsupportedOperationException();
      }

      public int size() {
         return collection.size();
      }

      public Object[] toArray() {
         return collection.toArray();
      }

      public <T> T[] toArray(T[] a) {
         return collection.toArray(a);
      }

      public String toString() {
         return collection.toString();
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

      public Iterator<E> reverseIterator() {
         return new ImmutableIteratorWrapper<E>(((ReversibleOrderedSet<? extends E>) collection).reverseIterator());
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

      public K getKey() {
         return key;
      }

      public V getValue() {
         return value;
      }

      public V setValue(V value) {
         throw new UnsupportedOperationException();
      }

      private static boolean eq(Object o1, Object o2) {
         return o1 == o2 || (o1 != null && o1.equals(o2));
      }

      @SuppressWarnings("unchecked")
      public boolean equals(Object o) {
         if (!(o instanceof Entry))
            return false;

         Entry<K, V> entry = (Entry<K, V>) o;
         return eq(entry.getKey(), key) && eq(entry.getValue(), value);
      }

      public int hashCode() {
         return hash;
      }

      public String toString() {
         return getKey() + "=" + getValue();
      }
   }

   /**
    * Immutable version of InternalCacheEntry for traversing data containers.
    */
   private static class ImmutableInternalCacheEntry implements InternalCacheEntry, Immutable {
      private final InternalCacheEntry entry;
      private final int hash;

      ImmutableInternalCacheEntry(InternalCacheEntry entry) {
         this.entry = entry;
         this.hash = entry.hashCode();
      }

      public Object getKey() {
         return entry.getKey();
      }

      public Object getValue() {
         return entry.getValue();
      }

      public Object setValue(Object value) {
         throw new UnsupportedOperationException();
      }

      @SuppressWarnings("unchecked")
      public boolean equals(Object o) {
         if (!(o instanceof InternalCacheEntry))
            return false;

         InternalCacheEntry entry = (InternalCacheEntry) o;
         return entry.equals(this.entry);
      }

      public int hashCode() {
         return hash;
      }

      public String toString() {
         return getKey() + "=" + getValue();
      }

      public boolean canExpire() {
         return entry.canExpire();
      }

      public long getCreated() {
         return entry.getCreated();
      }

      public long getExpiryTime() {
         return entry.getExpiryTime();
      }

      public long getLastUsed() {
         return entry.getLastUsed();
      }

      public boolean isExpired(long now) {
         return entry.isExpired(now);
      }

      public boolean isExpired() {
         return entry.isExpired();
      }

      public void setLifespan(long lifespan) {
         throw new UnsupportedOperationException();
      }

      public void setMaxIdle(long maxIdle) {
         throw new UnsupportedOperationException();
      }

      public InternalCacheValue toInternalCacheValue() {
         return new ImmutableInternalCacheValue(this);
      }

      public void touch() {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean undelete(boolean doUndelete) {
         throw new UnsupportedOperationException();
      }

      public void reincarnate() {
         throw new UnsupportedOperationException();
      }

      public void commit(DataContainer container) {
         throw new UnsupportedOperationException();
      }

      public long getLifespan() {
         return entry.getLifespan();
      }

      public long getMaxIdle() {
         return entry.getMaxIdle();
      }

      public boolean isChanged() {
         return entry.isChanged();
      }

      public boolean isCreated() {
         return entry.isCreated();
      }

      public boolean isNull() {
         return entry.isNull();
      }

      public boolean isRemoved() {
         return entry.isRemoved();
      }

      public boolean isEvicted() {
         return entry.isEvicted();
      }

      public boolean isValid() {
         return entry.isValid();
      }

      public void rollback() {
         throw new UnsupportedOperationException();
      }

      public void setCreated(boolean created) {
         throw new UnsupportedOperationException();
      }

      public void setRemoved(boolean removed) {
         throw new UnsupportedOperationException();
      }

      public void setEvicted(boolean evicted) {
         entry.setEvicted(evicted);
      }

      public void setValid(boolean valid) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean isLockPlaceholder() {
         return entry.isLockPlaceholder();
      }

      public InternalCacheEntry clone() {
         return new ImmutableInternalCacheEntry(entry.clone());
      }
   }

   private static class ImmutableInternalCacheValue implements InternalCacheValue, Immutable {
      private final ImmutableInternalCacheEntry entry;

      ImmutableInternalCacheValue(ImmutableInternalCacheEntry entry) {
         this.entry = entry;
      }

      public boolean canExpire() {
         return entry.canExpire();
      }

      public long getCreated() {
         return entry.getCreated();
      }

      public long getLastUsed() {
         return entry.getLastUsed();
      }

      public long getLifespan() {
         return entry.getLifespan();
      }

      public long getMaxIdle() {
         return entry.getMaxIdle();
      }

      public Object getValue() {
         return entry.getValue();
      }

      public boolean isExpired(long now) {
         return entry.isExpired(now);
      }

      public boolean isExpired() {
         return entry.isExpired();
      }

      public InternalCacheEntry toInternalCacheEntry(Object key) {
         return entry;
      }
   }

   private static class ImmutableEntrySetWrapper<K, V> extends ImmutableSetWrapper<Entry<K, V>> {
      private static final long serialVersionUID = 6378667653889667692L;

      @SuppressWarnings("unchecked")
      public ImmutableEntrySetWrapper(Set<? extends Entry<? extends K, ? extends V>> set) {
         super((Set<Entry<K, V>>) set);
      }

      public Object[] toArray() {
         Object[] array = new Object[collection.size()];
         int i = 0;
         for (Entry<K, V> entry : this)
            array[i++] = entry;
         return array;
      }

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

      public Iterator<Entry<K, V>> iterator() {
         return new ImmutableIteratorWrapper<Entry<K, V>>(collection.iterator()) {
            public Entry<K, V> next() {
               return new ImmutableEntry<K, V>(super.next());
            }
         };
      }
   }

   private static class ImmutableMapWrapper<K, V> implements Map<K, V>, Serializable, Immutable {
      private static final long serialVersionUID = 708144227046742221L;

      private Map<? extends K, ? extends V> map;

      public ImmutableMapWrapper(Map<? extends K, ? extends V> map) {
         this.map = map;
      }

      public void clear() {
         throw new UnsupportedOperationException();
      }

      public boolean containsKey(Object key) {
         return map.containsKey(key);
      }

      public boolean containsValue(Object value) {
         return map.containsValue(value);
      }

      public Set<Entry<K, V>> entrySet() {
         return new ImmutableEntrySetWrapper<K, V>(map.entrySet());
      }

      public boolean equals(Object o) {
         return map.equals(o);
      }

      public V get(Object key) {
         return map.get(key);
      }

      public int hashCode() {
         return map.hashCode();
      }

      public boolean isEmpty() {
         return map.isEmpty();
      }

      public Set<K> keySet() {
         return new ImmutableSetWrapper<K>(map.keySet());
      }

      public V put(K key, V value) {
         throw new UnsupportedOperationException();
      }

      public void putAll(Map<? extends K, ? extends V> t) {
         throw new UnsupportedOperationException();
      }

      public V remove(Object key) {
         throw new UnsupportedOperationException();
      }

      public int size() {
         return map.size();
      }

      public Collection<V> values() {
         return new ImmutableCollectionWrapper<V>(map.values());
      }

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
      @SuppressWarnings("unchecked")
      public Map readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Map map = new HashMap();
         MarshallUtil.unmarshallMap(map, input);
         return Immutables.immutableMapWrap(map);
      }

      @Override
      public Integer getId() {
         return Ids.IMMUTABLE_MAP;
      }

      @Override
      public Set<Class<? extends Map>> getTypeClasses() {
         return Util.<Class<? extends Map>>asSet(ImmutableMapWrapper.class);
      }
   }
   
   private static class ImmutableTypedProperties extends TypedProperties {
      
      ImmutableTypedProperties(TypedProperties properties) {
         super(properties);
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
      public synchronized void putAll(Map<? extends Object, ? extends Object> t) {
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
