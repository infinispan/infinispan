package org.infinispan.query.remote.impl.indexing;

import java.util.Set;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.compat.PassThroughTypeConverter;
import org.infinispan.compat.TypeConverter;
import org.infinispan.context.Flag;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.compat.BaseTypeConverterInterceptor;
import org.infinispan.notifications.cachelistener.CacheNotifier;

/**
 * Converts the (Protobuf encoded) binary values put in remote caches to a hibernate-search indexing-enabled wrapper object
 * that has the proper FieldBridge to decode the data and construct the Lucene document to be indexed.
 *
 * Only operations that have the flag Flag.OPERATION_HOTROD are intercepted.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class RemoteValueWrapperInterceptor<K, V> extends BaseTypeConverterInterceptor<K, V> {

   private final ProtobufValueWrapperTypeConverter protobufTypeConverter = new ProtobufValueWrapperTypeConverter();

   private final PassThroughTypeConverter passThroughTypeConverter = new PassThroughTypeConverter();

   @SuppressWarnings("unused")
   @Inject
   public void injectDependencies(CacheNotifier cacheNotifier) {
      cacheNotifier.setTypeConverter(protobufTypeConverter);
   }

   protected TypeConverter<Object, Object, Object, Object> determineTypeConverter(FlagAffectedCommand command) {
      Set<Flag> flags = command.getFlags();
      return flags != null && flags.contains(Flag.OPERATION_HOTROD) ? protobufTypeConverter : passThroughTypeConverter;
   }

   /**
    * A converter that wraps/unwraps the value (a byte[]) into a ProtobufValueWrapper.
    */
   private class ProtobufValueWrapperTypeConverter extends PassThroughTypeConverter {

      @Override
      public Object boxValue(Object value) {
         if (value instanceof WrappedByteArray) {
            return new ProtobufValueWrapper(((WrappedByteArray) value).getBytes());
         }
         return value;
      }

      @Override
      public Object unboxValue(Object target) {
         if (target instanceof ProtobufValueWrapper) {
            byte[] bytes = ((ProtobufValueWrapper) target).getBinary();
            return new WrappedByteArray(bytes);
         }
         return target;
      }
   }
}
