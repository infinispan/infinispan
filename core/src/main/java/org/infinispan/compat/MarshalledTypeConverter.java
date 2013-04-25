package org.infinispan.compat;

import org.infinispan.CacheException;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.marshall.jboss.GenericJBossMarshaller;

/**
 * Type converter for marshalled types which box keys into their
 * deserialized form, and unbox deserialized forms into their
 * serialized form.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public final class MarshalledTypeConverter
      implements TypeConverter<Object, Object, Object, Object> {

   // Use same marshaller as Hot Rod client
   // TODO: Make it configurable?
   private final StreamingMarshaller marshaller = new GenericJBossMarshaller();

   @Override
   public Object boxKey(Object key) {
      if (key instanceof byte[])
         return unmarshall((byte[]) key);
      else
         return key;
   }

   @Override
   public Object boxValue(Object key, Object value) {
      return boxKey(value); // boxing keys and values treated same way
   }

   private Object unmarshall(byte[] source) {
      try {
         return marshaller.objectFromByteBuffer(source);
      } catch (Exception e) {
         throw new CacheException("Unable to convert from byte buffer", e);
      }
   }

   @Override
   public Object unboxKey(Object target) {
      return marshall(target);
   }

   @Override
   public Object unboxValue(Object key, Object value) {
      // If key not binary, assume that return is not binary either
      if (value != null && key instanceof byte[])
         return marshall(value);
      else
         return value;
   }

   private Object marshall(Object source) {
      try {
         return marshaller.objectToByteBuffer(source);
      } catch (Exception e) {
         throw new CacheException("Unable to convert to byte buffer", e);
      }
   }

}
