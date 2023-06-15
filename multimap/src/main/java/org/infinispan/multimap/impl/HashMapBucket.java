package org.infinispan.multimap.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
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

   public Map<K, V> values() {
      return fromStore();
   }

   public V get(K k) {
      return values.get(new MultimapObjectWrapper<>(k));
   }

   public int size() {
      return values.size();
   }

   public Set<K> keySet() {
      Set<K> keys = new HashSet<>(values.size());
      for (MultimapObjectWrapper<K> key : values.keySet()) {
         keys.add(key.get());
      }
      return keys;
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
