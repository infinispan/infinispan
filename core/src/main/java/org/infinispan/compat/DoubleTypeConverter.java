package org.infinispan.compat;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.context.Flag;

/**
 * Simple converter that applies 2 converters to the given key or value.  This is useful when only 1 converter
 * can be provided but 2 are required.  The first converter decides if the converter can handle the invocation.
 * @author wburns
 * @since 9.0
 */
public class DoubleTypeConverter<K, V, K2, V2, K3, V3> implements TypeConverter<K, V, K3, V3> {
   private final TypeConverter<K, V, K2, V2> converter1;
   private final TypeConverter<K2, V2, K3, V3> converter2;

   public DoubleTypeConverter(TypeConverter<K, V, K2, V2> converter1, TypeConverter<K2, V2, K3, V3> converter2) {
      this.converter1 = converter1;
      this.converter2 = converter2;
   }

   @Override
   public K3 boxKey(K key) {
      K2 key2 = converter1.boxKey(key);
      return converter2.boxKey(key2);
   }

   @Override
   public V3 boxValue(V value) {
      V2 value2 = converter1.boxValue(value);
      return converter2.boxValue(value2);
   }

   @Override
   public K unboxKey(K3 target) {
      K2 key2 = converter2.unboxKey(target);
      return converter1.unboxKey(key2);
   }

   @Override
   public V unboxValue(V3 target) {
      V2 value2 = converter2.unboxValue(target);
      return converter1.unboxValue(value2);
   }

   @Override
   public boolean supportsInvocation(Flag flag) {
      return converter1.supportsInvocation(flag);
   }

   @Override
   public void setMarshaller(Marshaller marshaller) {
      converter1.setMarshaller(marshaller);
      converter2.setMarshaller(marshaller);
   }
}
