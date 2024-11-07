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

   private HashMapBucket(Map<MultimapObjectWrapper<K>, V> values) {
      this.values = values;
   }

   @ProtoFactory
   HashMapBucket(Collection<BucketEntry<K, V>> wrappedValues) {
      this.values = wrappedValues.stream()
            .collect(Collectors.toMap(e -> new MultimapObjectWrapper<>(e.getKey()), BucketEntry::getValue));
   }

   public static <K, V> HashMapBucket<K, V> create(Map<K, V> values) {
      return new HashMapBucket<>(toStore(values));
   }

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   Collection<BucketEntry<K, V>> getWrappedValues() {
      return values.entrySet().stream().map(BucketEntry::new).collect(Collectors.toList());
   }

   public HashMapBucketResponse<Integer, K, V> putAll(Map<K, V> map) {
      Map<MultimapObjectWrapper<K>, V> copied = new HashMap<>(values);
      int res = 0;
      for (Map.Entry<K, V> entry : map.entrySet()) {
         V prev = copied.put(new MultimapObjectWrapper<>(entry.getKey()), entry.getValue());
         if (prev == null) res++;
      }
      return new HashMapBucketResponse<>(res, new HashMapBucket<>(copied));
   }

   public HashMapBucketResponse<Integer, K, V> putIfAbsent(Map<K, V> map) {
      ByRef.Integer created = new ByRef.Integer(0);
      Map<MultimapObjectWrapper<K>, V> copied = new HashMap<>(values);
      for (Map.Entry<K, V> entry : map.entrySet()) {
         // The `values` map can have null values, we use compute instead of putIfAbsent.
         copied.computeIfAbsent(new MultimapObjectWrapper<>(entry.getKey()), ignore -> {
            created.inc();
            return entry.getValue();
         });
      }
      return new HashMapBucketResponse<>(created.get(), new HashMapBucket<>(copied));
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

   public HashMapBucketResponse<Integer, K, V> removeAll(Collection<K> keys) {
      int res = 0;
      Map<MultimapObjectWrapper<K>, V> copied = new HashMap<>(values.size());
      for (Map.Entry<MultimapObjectWrapper<K>, V> entry : values.entrySet()) {
         if (containsKey(keys, entry.getKey())) {
            res++;
            continue;
         }

         copied.put(entry.getKey(), entry.getValue());
      }
      return new HashMapBucketResponse<>(res, new HashMapBucket<>(copied));
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
   public HashMapBucket<K, V> replace(K key, V expected, V replacement) {
      MultimapObjectWrapper<K> storeKey = new MultimapObjectWrapper<>(key);
      V current = values.get(storeKey);

      if (!equalValues(current, expected)) return null;
      if (equalValues(current, replacement)) return this;

      if (values.isEmpty()) {
         Map<MultimapObjectWrapper<K>, V> copied = new HashMap<>();
         copied.put(storeKey, replacement);
         return new HashMapBucket<>(copied);
      }

      Map<MultimapObjectWrapper<K>, V> copied = new HashMap<>(values);
      if (replacement == null) {
         copied.remove(storeKey, expected);
      } else {
         copied.put(storeKey, replacement);
      }
      return new HashMapBucket<>(copied);
   }

   private boolean equalValues(V one, Object other) {
      boolean eq;
      if (one instanceof byte[] && other instanceof byte[])
         eq = Arrays.equals((byte[]) one, (byte[]) other);
      else
         eq = Objects.equals(one, other);

      return eq;
   }

   private static <K, V> Map<MultimapObjectWrapper<K>, V> toStore(Map<K, V> raw) {
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

   private boolean containsKey(Collection<K> keys, MultimapObjectWrapper<K> entry) {
      for (K key : keys) {
         if (MultimapObjectWrapper.wrappedEquals(entry.get(), key))
            return true;
      }
      return false;
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

   public record HashMapBucketResponse<R, K, V>(R response, HashMapBucket<K, V> bucket) { }
}
