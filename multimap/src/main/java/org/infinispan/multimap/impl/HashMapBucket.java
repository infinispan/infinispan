package org.infinispan.multimap.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableUserObject;
import org.infinispan.multimap.internal.MultimapDataConverter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_HASH_MAP_BUCKET)
public class HashMapBucket<K, V> {

   final Map<Object, Object> values;

   public HashMapBucket() {
      this.values = Collections.emptyMap();
   }

   public HashMapBucket(K key, V value) {
      this.values = new HashMap<>();
      values.put(key, value);
   }

   private HashMapBucket(Map<K, V> values, MultimapDataConverter<K, V> converter) {
      this.values = toStore(values, converter);
   }

   @ProtoFactory
   HashMapBucket(Collection<BucketEntry<K, V>> wrappedValues) {
      this.values = wrappedValues.stream().collect(Collectors.toMap(BucketEntry::getKey, BucketEntry::getValue));
   }

   public static <K, V> HashMapBucket<K, V> create(Map<K, V> values, MultimapDataConverter<K, V> converter) {
      return new HashMapBucket<>(values, converter);
   }

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   Collection<BucketEntry<Object, Object>> getWrappedValues() {
      return values.entrySet().stream().map(BucketEntry::new).collect(Collectors.toList());
   }

   public int putAll(Map<K, V> map, MultimapDataConverter<K, V> converter) {
      int res = 0;
      for (Map.Entry<K, V> entry : map.entrySet()) {
         Object prev = values.put(converter.convertKeyToStore(entry.getKey()), converter.convertValueToStore(entry.getValue()));
         if (prev == null) res++;
      }
      return res;
   }

   public Map<K, V> values(MultimapDataConverter<K, V> converter) {
      return fromStore(converter);
   }

   public V get(K k, MultimapDataConverter<K, V> converter) {
      Object value = values.get(converter.convertKeyToStore(k));
      if (value == null) return null;
      return converter.convertValueFromStore(value);
   }

   public int size() {
      return values.size();
   }

   private Map<Object, Object> toStore(Map<K, V> raw, MultimapDataConverter<K, V> converter) {
      Map<Object, Object> converted = new HashMap<>();
      for (Map.Entry<K, V> entry : raw.entrySet()) {
         converted.put(converter.convertKeyToStore(entry.getKey()), converter.convertValueToStore(entry.getValue()));
      }
      return converted;
   }

   private Map<K, V> fromStore(MultimapDataConverter<K, V> converter) {
      Map<K, V> converted = new HashMap<>();
      for (Map.Entry<Object, Object> entry : values.entrySet()) {
         converted.put(converter.convertKeyFromStore(entry.getKey()), converter.convertValueFromStore(entry.getValue()));
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

      private BucketEntry(Map.Entry<K, V> entry) {
         this(entry.getKey(), entry.getValue());
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
