package org.infinispan.query.remote;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.commons.CacheException;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;

import java.io.IOException;

/**
 * A sub-class of ReflectionMatcher that is able to lookup classes by their protobuf type name and can work when
 * compatibility mode is used.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class CompatibilityReflectionMatcher extends ReflectionMatcher {

   private final SerializationContext serializationContext;

   public CompatibilityReflectionMatcher(final SerializationContext serializationContext) {
      super(new EntityNamesResolver() {
         @Override
         public Class<?> getClassFromName(String entityName) {
            try {
               MessageMarshaller messageMarshaller = (MessageMarshaller) serializationContext.getMarshaller(entityName);
               return messageMarshaller.getJavaClass();
            } catch (Exception e) {
               return null;
            }
         }
      });
      this.serializationContext = serializationContext;
   }

   /**
    * Marshals the instance using Protobuf.
    *
    * @param instance never null
    * @return the converted/decorated instance
    */
   @Override
   protected Object convert(Object instance) {
      try {
         return ProtobufUtil.toWrappedByteArray(serializationContext, instance);
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }
}
