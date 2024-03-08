package org.infinispan.multimap.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.ByRef;
import org.infinispan.marshall.protostream.impl.MarshallableUserObject;
import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_HASH_MAP_BUCKET)
public class HashMapBucket<K, V> {

   final Map<MultimapObjectWrapper<K>, V> values;

   private HashMapBucket(Map<K, V> values) {
      this.values = toStore(values);
   }

   @ProtoFactory
   HashMapBucket(Collection<BucketEntry<K, V>> wrappedValues) {
      this.values = wrappedValues.stream()
            .collect(Collectors.toMap(e -> new MultimapObjectWrapper<>(e.getKey()), BucketEntry::getValue));
   }

   public static <K, V> HashMapBucket<K, V> create(Map<K, V> values) {
      return new HashMapBucket<>(values);
   }

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   Collection<BucketEntry<K, V>> getWrappedValues() {
      return values.entrySet().stream().map(BucketEntry::new).collect(Collectors.toList());
   }

   public int putAll(Map<K, V> map) {
      int res = 0;
      for (Map.Entry<K, V> entry : map.entrySet()) {
         V prev = values.put(new MultimapObjectWrapper<>(entry.getKey()), entry.getValue());
         if (prev == null) res++;
      }
      return res;
   }

   public int putIfAbsent(Map<K, V> map) {
      ByRef.Integer created = new ByRef.Integer(0);
      for (Map.Entry<K, V> entry : map.entrySet()) {
         // The `values` map can have null values, we use compute instead of putIfAbsent.
         values.computeIfAbsent(new MultimapObjectWrapper<>(entry.getKey()), ignore -> {
            created.inc();
            return entry.getValue();
         });
      }
      return created.get();
   }

   public Map<K, V> getAll(Set<K> keys) {
      // We can have null vales here, so we need HashMap.
      Map<K, V> response = new HashMap<>(keys.size());
      for (K key : keys) {
         response.put(key, values.get(new MultimapObjectWrapper<>(key)));
      }
      return response;
   }

   public Map<K, V> converted() {
      return fromStore();
   }

   public boolean isEmpty() {
      return values.isEmpty();
   }

   public int removeAll(Collection<K> keys) {
      int res = 0;
      for (K key : keys) {
         Object prev = values.remove(new MultimapObjectWrapper<>(key));
         if (prev != null) res++;
      }
      return res;
   }

   public V get(K k) {
      return values.get(new MultimapObjectWrapper<>(k));
   }

   public int size() {
      return values.size();
   }

   public Collection<V> values() {
      return new ArrayList<>(values.values());
   }

   public Set<K> keySet() {
      Set<K> keys = new HashSet<>(values.size());
      for (MultimapObjectWrapper<K> key : values.keySet()) {
         keys.add(key.get());
      }
      return keys;
   }

   public boolean containsKey(K key) {
      return values.containsKey(new MultimapObjectWrapper<>(key));
   }

   /**
    * We do not use the original replace method here. Our implementation allows to create and delete entries.
    */
   public boolean replace(K key, V expected, V replacement) {
      MultimapObjectWrapper<K> storeKey = new MultimapObjectWrapper<>(key);
      V current = values.get(storeKey);
      if (current == null && expected == null && replacement != null) {
         values.put(storeKey, replacement);
         return true;
      }

      if (!equalValues(current, expected)) return false;

      if (replacement == null) {
         values.remove(storeKey);
      } else {
         values.put(storeKey, replacement);
      }
      return true;
   }

   private boolean equalValues(V one, Object other) {
      boolean eq;
      if (one instanceof byte[] && other instanceof byte[])
         eq = Arrays.equals((byte[]) one, (byte[]) other);
      else
         eq = Objects.equals(one, other);

      return eq;
   }

   private Map<MultimapObjectWrapper<K>, V> toStore(Map<K, V> raw) {
      Map<MultimapObjectWrapper<K>, V> converted = new HashMap<>();
      for (Map.Entry<K, V> entry : raw.entrySet()) {
         converted.put(new MultimapObjectWrapper<>(entry.getKey()), entry.getValue());
      }
      return converted;
   }

   private Map<K, V> fromStore() {
      Map<K, V> converted = new HashMap<>();
      for (Map.Entry<MultimapObjectWrapper<K>, V> entry : values.entrySet()) {
         converted.put(entry.getKey().get(), entry.getValue());
      }
      return converted;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      try {
         HashMapBucket<?, ?> that = (HashMapBucket<?, ?>) o;
         if (values.size() != that.values.size()) return false;

         for (Map.Entry<MultimapObjectWrapper<K>, V> e : values.entrySet()) {
            MultimapObjectWrapper<K> key = e.getKey();
            V value = e.getValue();
            boolean eq = equalValues(value, that.values.get(key));

            if (!eq) return false;
         }
      } catch (ClassCastException ignore) {
         return false;
      }

      return true;
   }

   @Override
   public int hashCode() {
      return Objects.hash(values);
   }

   @ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_HASH_MAP_BUCKET_ENTRY)
   static class BucketEntry<K, V> {
      final K key;
      final V value;

      private BucketEntry(K key, V value) {
         this.key = key;
         this.value = value;
      }

      private BucketEntry(Map.Entry<MultimapObjectWrapper<K>, V> entry) {
         this(entry.getKey().get(), entry.getValue());
      }

      @ProtoFactory
      BucketEntry(MarshallableUserObject<K> wrappedKey, MarshallableUserObject<V> wrappedValue) {
         this(wrappedKey.get(), wrappedValue.get());
      }

      @ProtoField(number = 1)
      MarshallableUserObject<K> wrappedKey() {
         return new MarshallableUserObject<>(key);
      }

      public K getKey() {
         return key;
      }

      @ProtoField(number = 2)
      MarshallableUserObject<V> wrappedValue() {
         return new MarshallableUserObject<>(value);
      }

      public V getValue() {
         return value;
      }
   }
}
