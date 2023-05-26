package org.infinispan.multimap.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
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

   // We don't transfer the converter in the proto message.
   private MultimapDataConverter<K, V> converter = null;

   public HashMapBucket() {
      this.values = Collections.emptyMap();
   }

   public HashMapBucket(MultimapDataConverter<K, V> converter, K key, V value) {
      this.values = new HashMap<>();
      values.put(key, value);
      this.converter = converter;
   }

   private HashMapBucket(MultimapDataConverter<K, V> converter, Map<K, V> values) {
      this.converter = converter;
      this.values = toStore(values);
   }

   @ProtoFactory
   HashMapBucket(Collection<BucketEntry<Object, Object>> wrappedValues) {
      this.values = wrappedValues.stream().collect(Collectors.toMap(Function.identity(), Function.identity()));
      this.converter = null;
   }

   public static <K, V> HashMapBucket<K, V> create(MultimapDataConverter<K, V> converter, Map<K, V> values) {
      return new HashMapBucket<>(converter, values);
   }

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   Collection<BucketEntry<Object, Object>> getWrappedValues() {
      return values.entrySet().stream().map(BucketEntry::new).collect(Collectors.toList());
   }

   public Map<K, V> putAll(Map<K, V> map) {
      Map<K, V> res = new HashMap<>();
      for (Map.Entry<K, V> entry : map.entrySet()) {
         Object prev = values.put(converter.convertKeyToStore(entry.getKey()), converter.convertValueToStore(entry.getValue()));
         // TODO if return ignored, we can avoid converting.
         if (prev != null) {
            res.put(entry.getKey(), converter.convertValueFromStore(prev));
         }
      }
      return res;
   }

   public Map<K, V> values() {
      return fromStore();
   }

   public HashMapBucket<K, V> withConverter(MultimapDataConverter<K, V> converter) {
      this.converter = converter;
      return this;
   }

   private Map<Object, Object> toStore(Map<K, V> raw) {
      if (converter == null) {
         return (Map) raw;
      }

      Map<Object, Object> converted = new HashMap<>();
      for (Map.Entry<K, V> entry : raw.entrySet()) {
         converted.put(converter.convertKeyToStore(entry.getKey()), converter.convertValueToStore(entry.getValue()));
      }
      return converted;
   }

   private Map<K, V> fromStore() {
      if (converter == null) {
         return (Map) values;
      }

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

      @ProtoField(number = 2)
      MarshallableUserObject<V> wrappedValue() {
         return new MarshallableUserObject<>(value);
      }
   }
}
