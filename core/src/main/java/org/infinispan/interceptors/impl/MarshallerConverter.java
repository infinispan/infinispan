package org.infinispan.interceptors.impl;

import java.io.IOException;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.compat.TypeConverter;
import org.infinispan.context.Flag;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * @author wburns
 * @since 9.0
 */
public class MarshallerConverter implements TypeConverter<Object, Object, Object, Object> {
   private final StreamingMarshaller marshaller;
   public MarshallerConverter(StreamingMarshaller marshaller) {
      this.marshaller = marshaller;
   }
   @Override
   public Object unboxKey(Object key) {
      try {
         return key instanceof WrappedByteArray ? marshaller.objectFromByteBuffer(((WrappedByteArray) key).getBytes()) : key;
      } catch (IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
   }

   @Override
   public Object unboxValue(Object value) {
      return unboxKey(value);
   }

   @Override
   public Object boxKey(Object target) {
      if (target == null || isTypeExcluded(target.getClass())) {
         return target;
      }
      try {
         return new WrappedByteArray(marshaller.objectToByteBuffer(target));
      } catch (IOException | InterruptedException e) {
         throw new CacheException(e);
      }
   }

   @Override
   public Object boxValue(Object target) {
      return boxKey(target);
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

   public static boolean isTypeExcluded(Class<?> type) {
      return type.equals(String.class) || type.isPrimitive() ||
            type.equals(Boolean.class) || type.equals(Character.class) ||
            type.equals(Byte.class) || type.equals(Short.class) || type.equals(Integer.class) ||
            type.equals(Long.class) || type.equals(Float.class) || type.equals(Double.class) ||
            (type.isArray() && isTypeExcluded(type.getComponentType())) || type.equals(GlobalTransaction.class) || Address.class.isAssignableFrom(type) ||
            ReplicableCommand.class.isAssignableFrom(type) || type.equals(WrappedByteArray.class);
   }
}
