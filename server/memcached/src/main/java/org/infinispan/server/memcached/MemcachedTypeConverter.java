package org.infinispan.server.memcached;

import java.io.IOException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.compat.TypeConverter;
import org.infinispan.context.Flag;

/**
 * Type converter that transforms Memcached data so that it can be accessible
 * via other endpoints.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MemcachedTypeConverter implements TypeConverter<String, Object, String, Object> {
   // Default marshaller needed in case no custom marshaller is set
   // (e.g. not using Spy Memcached client). This is because in compatibility
   // mode, data is stored unmarshalled, so when returning data to Memcached
   // clients, it needs to be marshalled to fulfill the Memcached protocol.
   //
   // A generic marshaller using Java Serialization is used by default, since
   // that's the safest bet to support alternative Java Memcached clients
   private Marshaller marshaller = new JavaSerializationMarshaller();

   @Override
   public String boxKey(String key) {
      return key;
   }

   @Override
   public Object boxValue(Object value) {
      return unmarshall(value);
   }

   @Override
   public String unboxKey(String target) {
      return target;
   }

   @Override
   public Object unboxValue(Object target) {
      return marshall(target);
   }

   @Override
   public boolean supportsInvocation(Flag flag) {
      return flag == Flag.OPERATION_MEMCACHED;
   }

   @Override
   public void setMarshaller(Marshaller marshaller) {
      this.marshaller = marshaller;
   }

   private Object unmarshall(Object source) {
      if (source instanceof byte[]) {
         try {
            return marshaller.objectFromByteBuffer((byte[]) source);
         } catch (IOException | ClassNotFoundException e) {
            throw new CacheException(e);
         }
      }
      return source;
   }

   private byte[] marshall(Object source) {
      if (source != null) {
         try {
            return marshaller.objectToByteBuffer(source);
         } catch (IOException | InterruptedException e) {
            throw new CacheException(e);
         }
      }
      return null;
   }
}
