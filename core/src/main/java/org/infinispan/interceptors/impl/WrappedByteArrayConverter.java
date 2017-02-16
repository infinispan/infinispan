package org.infinispan.interceptors.impl;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.compat.TypeConverter;
import org.infinispan.context.Flag;

/**
 * Converter that will convert a byte[] into a {@linke WrappedByteArray} otherwise returns object as is
 * @author wburns
 * @since 9.0
 */
public class WrappedByteArrayConverter implements TypeConverter<Object, Object, Object, Object> {
   @Override
   public Object unboxKey(Object key) {
      return key instanceof WrappedByteArray ? ((WrappedByteArray) key).getBytes() : key;
   }

   @Override
   public Object unboxValue(Object value) {
      return value instanceof WrappedByteArray ? ((WrappedByteArray) value).getBytes() : value;
   }

   @Override
   public Object boxKey(Object target) {
      return target instanceof byte[] ? new WrappedByteArray((byte[]) target) : target;
   }

   @Override
   public Object boxValue(Object target) {
      return target instanceof byte[] ? new WrappedByteArray((byte[]) target) : target;
   }

   @Override
   public boolean supportsInvocation(Flag flag) {
      // Shouldn't be used
      return false;
   }

   @Override
   public void setMarshaller(Marshaller marshaller) {
      // Do nothing
   }
}
