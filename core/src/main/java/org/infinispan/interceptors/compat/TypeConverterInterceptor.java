package org.infinispan.interceptors.compat;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Set;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.Util;
import org.infinispan.compat.TypeConverter;
import org.infinispan.context.Flag;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An interceptor that applies type conversion to the data stored in the cache.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class TypeConverterInterceptor<K, V> extends BaseTypeConverterInterceptor<K, V> {

   // No need for a REST type converter since the REST server itself does
   // the hard work of converting from one content type to the other

   private TypeConverter<Object, Object, Object, Object> hotRodConverter;
   private TypeConverter<Object, Object, Object, Object> memcachedConverter;
   private TypeConverter<Object, Object, Object, Object> embeddedConverter;

   @SuppressWarnings("unchecked")
   public TypeConverterInterceptor(Marshaller marshaller) {
      Collection<TypeConverter> converters = ServiceFinder.load(TypeConverter.class);
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
      private static final Log log = LogFactory.getLog(EmbeddedTypeConverter.class);

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

         if (target instanceof byte[]) {
            // Try standard deserialization
            try {
               ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream((byte[]) target));
               return ois.readObject();
            } catch (Exception ee) {
               if (log.isDebugEnabled())
                  log.debugf("Standard deserialization not in use for %s", Util.printArray((byte[]) target));
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
