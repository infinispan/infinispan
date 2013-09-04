package org.infinispan.query.remote.indexing;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.compat.TypeConverter;
import org.infinispan.context.Flag;
import org.infinispan.interceptors.compat.BaseTypeConverterInterceptor;

import java.util.Set;

/**
 * Converts the (Protobuf encoded) binary values put in remote caches to a hibernate-search indexing-enabled wrapper object
 * that has the proper FieldBridge to decode the data and construct the Lucene document to be indexed.
 *
 * Only operations that have the flag Flag.OPERATION_HOTROD are intercepted.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public class RemoteValueWrapperInterceptor extends BaseTypeConverterInterceptor {

   private final ProtobufValueWrapperTypeConverter protobufTypeConverter = new ProtobufValueWrapperTypeConverter();

   private final PassThroughTypeConverter passThroughTypeConverter = new PassThroughTypeConverter();

   protected TypeConverter<Object, Object, Object, Object> determineTypeConverter(Set<Flag> flags) {
      return flags != null && flags.contains(Flag.OPERATION_HOTROD) ? protobufTypeConverter : passThroughTypeConverter;
   }

   /**
    * A no-op converter.
    */
   private static class PassThroughTypeConverter implements TypeConverter<Object, Object, Object, Object> {

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
         return target;
      }

      @Override
      public Object unboxValue(Object target) {
         return target;
      }

      @Override
      public boolean supportsInvocation(Flag flag) {
         return false;
      }

      @Override
      public void setMarshaller(Marshaller marshaller) {
      }
   }

   /**
    * A converter that wraps/unwraps the value (a byte[]) into a ProtobufValueWrapper.
    */
   private static class ProtobufValueWrapperTypeConverter extends PassThroughTypeConverter {

      @Override
      public Object boxValue(Object value) {
         if (value instanceof byte[]) {
            return new ProtobufValueWrapper((byte[]) value);
         }
         return value;
      }

      @Override
      public Object unboxValue(Object target) {
         if (target instanceof ProtobufValueWrapper) {
            return ((ProtobufValueWrapper) target).getBinary();
         }
         return target;
      }
   }
}
