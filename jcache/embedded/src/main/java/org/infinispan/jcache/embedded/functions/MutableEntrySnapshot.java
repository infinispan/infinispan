package org.infinispan.jcache.embedded.functions;

import javax.cache.processor.MutableEntry;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.JCACHE_MUTABLE_ENTRY_SNAPSHOT)
public class MutableEntrySnapshot<K, V> implements MutableEntry<K, V> {
   private final K key;
   private final V value;

   public MutableEntrySnapshot(K key, V value) {
      this.key = key;
      this.value = value;
   }

   @ProtoFactory
   MutableEntrySnapshot(MarshallableObject<K> wrappedKey, MarshallableObject<V> wrappedValue) {
      this(MarshallableObject.unwrap(wrappedKey), MarshallableObject.unwrap(wrappedValue));
   }

   @ProtoField(1)
   MarshallableObject<K> getWrappedKey() {
      return MarshallableObject.create(key);
   }

   @ProtoField(2)
   MarshallableObject<V> getWrappedValue() {
      return MarshallableObject.create(value);
   }

   @Override
   public boolean exists() {
      return value != null;
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException();
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
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrap(this, clazz);
   }

   @Override
   public void setValue(V value) {
      throw new UnsupportedOperationException();
   }
}
