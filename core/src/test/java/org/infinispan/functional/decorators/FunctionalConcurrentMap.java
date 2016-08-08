package org.infinispan.functional.decorators;

import static org.infinispan.commons.marshall.MarshallableFunctions.removeIfValueEqualsReturnBoolean;
import static org.infinispan.commons.marshall.MarshallableFunctions.removeReturnPrevOrNull;
import static org.infinispan.commons.marshall.MarshallableFunctions.returnReadOnlyFindIsPresent;
import static org.infinispan.commons.marshall.MarshallableFunctions.returnReadOnlyFindOrNull;
import static org.infinispan.commons.marshall.MarshallableFunctions.setValueConsumer;
import static org.infinispan.commons.marshall.MarshallableFunctions.setValueIfAbsentReturnPrevOrNull;
import static org.infinispan.commons.marshall.MarshallableFunctions.setValueIfEqualsReturnBoolean;
import static org.infinispan.commons.marshall.MarshallableFunctions.setValueIfPresentReturnPrevOrNull;
import static org.infinispan.commons.marshall.MarshallableFunctions.setValueReturnPrevOrNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.FunctionalMap.ReadOnlyMap;
import org.infinispan.commons.api.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.commons.api.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.commons.api.functional.Listeners.ReadWriteListeners;
import org.infinispan.commons.api.functional.Listeners.WriteListeners;
import org.infinispan.commons.api.functional.Param.FutureMode;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.stream.CacheCollectors;

/**
 * A {@link ConcurrentMap} implementation that uses the operations exposed by
 * {@link ReadOnlyMap}, {@link WriteOnlyMap} and {@link ReadWriteMap}, and
 * validates their usefulness.
 */
public final class FunctionalConcurrentMap<K, V> implements ConcurrentMap<K, V>, FunctionalListeners<K, V> {

   final ReadOnlyMap<K, V> readOnly;
   final WriteOnlyMap<K, V> writeOnly;
   final ReadWriteMap<K, V> readWrite;

   // Rudimentary constructor, we'll provide more idiomatic construction
   // via main Infinispan class which is still to be defined
   private FunctionalConcurrentMap(FunctionalMapImpl<K, V> map) {
      FunctionalMapImpl<K, V> blockingMap = map.withParams(FutureMode.COMPLETED);
      this.readOnly = ReadOnlyMapImpl.create(blockingMap);
      this.writeOnly = WriteOnlyMapImpl.create(blockingMap);
      this.readWrite = ReadWriteMapImpl.create(blockingMap);
   }

   public static <K, V> FunctionalConcurrentMap<K, V> create(AdvancedCache<K, V> cache) {
      return new FunctionalConcurrentMap<>(FunctionalMapImpl.create(cache));
   }

   @Override
   public ReadWriteListeners<K, V> readWriteListeners() {
      return readWrite.listeners();
   }

   @Override
   public WriteListeners<K, V> writeOnlyListeners() {
      return writeOnly.listeners();
   }

   @Override
   public int size() {
      return (int) readOnly.keys().count();
   }

   @Override
   public boolean isEmpty() {
      return !readOnly.keys().findAny().isPresent();
   }

   @Override
   public boolean containsKey(Object key) {
      return await(readOnly.eval(toK(key), returnReadOnlyFindIsPresent()));
   }

   @Override
   public boolean containsValue(Object value) {
      return readOnly.entries().anyMatch(ro -> ro.get().equals(value));
   }

   @Override
   public V get(Object key) {
      return await(readOnly.eval(toK(key), returnReadOnlyFindOrNull()));
   }

   @SuppressWarnings("unchecked")
   private K toK(Object key) {
      return (K) key;
   }

   @SuppressWarnings("unchecked")
   private V toV(Object value) {
      return (V) value;
   }

   @Override
   public V put(K key, V value) {
      return await(readWrite.eval(toK(key), value, setValueReturnPrevOrNull()));
   }

   @Override
   public V remove(Object key) {
      return await(readWrite.eval(toK(key), removeReturnPrevOrNull()));
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      // Since blocking is in use, there's no need to consume the iterator.
      // With blocking, the iterator gets pro-actively consumed, and the
      // return offers the possibility to re-iterate by the user.
      // Since the iteration here has no result, we can skip the iteration altogether.
      writeOnly.evalMany(m, setValueConsumer());
   }

   @Override
   public void clear() {
      await(writeOnly.truncate());
   }

   @Override
   public Set<K> keySet() {
      return readOnly.keys().collect(CacheCollectors.serializableCollector(() -> Collectors.toSet()));
   }

   @Override
   public Collection<V> values() {
      return readOnly.entries().collect(ArrayList::new, (l, v) -> l.add(v.get()), ArrayList::addAll);
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      return readOnly.entries().collect(HashSet::new, (s, ro) ->
         s.add(new FunctionalMapEntry<>(ro, writeOnly)), HashSet::addAll);
   }

   @Override
   public V putIfAbsent(K key, V value) {
      return await(readWrite.eval(toK(key), value, setValueIfAbsentReturnPrevOrNull()));
   }

   @Override
   public boolean remove(Object key, Object value) {
      return await(readWrite.eval(toK(key), toV(value), removeIfValueEqualsReturnBoolean()));
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return await(readWrite.eval(toK(key), newValue, setValueIfEqualsReturnBoolean(oldValue)));
   }

   @Override
   public V replace(K key, V value) {
      return await(readWrite.eval(toK(key), value, setValueIfPresentReturnPrevOrNull()));
   }

   public static <T> T await(CompletableFuture<T> cf) {
      try {
         return cf.get();
      } catch (InterruptedException | ExecutionException e) {
         throw new Error(e);
      }
   }

   private static final class FunctionalMapEntry<K, V> implements Entry<K, V> {
      final ReadEntryView<K, V> view;
      final WriteOnlyMap<K, V> writeOnly;

      private FunctionalMapEntry(ReadEntryView<K, V> view, WriteOnlyMap<K, V> writeOnly) {
         this.view = view;
         this.writeOnly = writeOnly;
      }

      @Override
      public K getKey() {
         return view.key();
      }

      @Override
      public V getValue() {
         return view.get();
      }

      @Override
      public V setValue(V value) {
         V prev = view.get();
         writeOnly.eval(view.key(), value, setValueConsumer());
         return prev;
      }

      @Override
      public boolean equals(Object o) {
         if (o == this)
            return true;
         if (o instanceof Entry) {
            Entry<?, ?> e = (Entry<?, ?>) o;
            if (Objects.equals(view.key(), e.getKey()) &&
               Objects.equals(view.get(), e.getValue()))
               return true;
         }
         return false;
      }

      @Override
      public int hashCode() {
         return view.hashCode();
      }

      @Override
      public String toString() {
         return "FunctionalMapEntry{" +
            "view=" + view +
            '}';
      }
   }
}
