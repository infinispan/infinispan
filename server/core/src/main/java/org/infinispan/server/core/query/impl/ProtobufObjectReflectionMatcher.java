package org.infinispan.server.core.query.impl;

import java.io.IOException;

import org.infinispan.commons.CacheException;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.objectfilter.impl.ReflectionMatcher;
import org.infinispan.query.objectfilter.impl.syntax.parser.EntityNameResolver;

/**
 * A sub-class of ReflectionMatcher that is able to lookup classes by their protobuf type name and can work with object
 * storage.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
final class ProtobufObjectReflectionMatcher extends ReflectionMatcher {

   private final SerializationContext serializationContext;

   private ProtobufObjectReflectionMatcher(EntityNameResolver entityNameResolver, SerializationContext serializationContext) {
      super(entityNameResolver);
      this.serializationContext = serializationContext;
   }

   static ProtobufObjectReflectionMatcher create(EntityNameResolver<Class<?>> entityNameResolver, SerializationContext ctx) {
      return new ProtobufObjectReflectionMatcher(entityNameResolver, ctx);
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
