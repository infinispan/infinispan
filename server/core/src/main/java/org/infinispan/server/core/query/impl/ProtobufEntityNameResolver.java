package org.infinispan.server.core.query.impl;

import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.objectfilter.impl.syntax.parser.EntityNameResolver;

/**
 * @author anistor@redhat.com
 * @since 9.1
 */
final class ProtobufEntityNameResolver implements EntityNameResolver<Class<?>> {

   private final SerializationContext serializationContext;

   ProtobufEntityNameResolver(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   @Override
   public Class<?> resolve(String entityName) {
      return serializationContext.canMarshall(entityName) ? serializationContext.getMarshaller(entityName).getJavaClass() : null;
   }
}
