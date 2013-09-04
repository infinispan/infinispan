package org.infinispan.interceptors.compat;

import org.infinispan.compat.TypeConverter;
import org.infinispan.context.Flag;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;

import java.util.ServiceLoader;
import java.util.Set;

/**
 * An interceptor that applies type conversion to the data stored in the cache.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class TypeConverterInterceptor extends BaseTypeConverterInterceptor {

   // No need for a REST type converter since the REST server itself does
   // the hard work of converting from one content type to the other

   private TypeConverter<Object, Object, Object, Object> hotRodConverter;
   private TypeConverter<Object, Object, Object, Object> memcachedConverter;
   private TypeConverter<Object, Object, Object, Object> embeddedConverter;

   @SuppressWarnings("unchecked")
   public TypeConverterInterceptor(Marshaller marshaller) {
      ServiceLoader<TypeConverter> converters = ServiceLoader.load(TypeConverter.class);
      for (TypeConverter converter : converters) {
         if (converter.supportsInvocation(Flag.OPERATION_HOTROD)) {
            hotRodConverter = setConverterMarshaller(converter, marshaller);
         } else if (converter.supportsInvocation(Flag.OPERATION_MEMCACHED)) {
            memcachedConverter = setConverterMarshaller(converter, marshaller);
         }
      }
      embeddedConverter = setConverterMarshaller(new EmbeddedTypeConverter(), marshaller);
   }

   private TypeConverter setConverterMarshaller(TypeConverter converter, Marshaller marshaller) {
      if (marshaller != null)
         converter.setMarshaller(marshaller);

      return converter;
   }

   protected TypeConverter<Object, Object, Object, Object> determineTypeConverter(Set<Flag> flags) {
      if (flags != null) {
         if (flags.contains(Flag.OPERATION_HOTROD))
            return hotRodConverter;
         else if (flags.contains(Flag.OPERATION_MEMCACHED))
            return memcachedConverter;
      }

      return embeddedConverter;
   }

   private static class EmbeddedTypeConverter
         implements TypeConverter<Object, Object, Object, Object> {

      private Marshaller marshaller;

      @Override
      public Object boxKey(Object key) {
         return key;
      }

      @Override
      public Object boxValue(Object value) {
         return value;
      }

      @Override
      public Object unboxKey(Object target) {
         return unboxValue(target);
      }

      @Override
      public Object unboxValue(Object target) {
         if (marshaller != null && target instanceof byte[]) {
            try {
               return marshaller.objectFromByteBuffer((byte[]) target);
            } catch (Exception e) {
               throw new CacheException("Unable to unmarshall return value");
            }
         }

         return target;
      }

      @Override
      public boolean supportsInvocation(Flag flag) {
         return false;
      }

      @Override
      public void setMarshaller(Marshaller marshaller) {
         this.marshaller = marshaller;
      }

   }

}
