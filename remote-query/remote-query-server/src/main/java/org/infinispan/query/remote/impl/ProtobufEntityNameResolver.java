package org.infinispan.query.remote.impl;

import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.protostream.SerializationContext;

/**
 * @author anistor@redhat.com
 * @since 9.1
 */
final class ProtobufEntityNameResolver implements EntityNameResolver {

   private final SerializationContext serializationContext;

   ProtobufEntityNameResolver(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   @Override
   public Class<?> resolve(String entityName) {
      return serializationContext.canMarshall(entityName) ? serializationContext.getMarshaller(entityName).getJavaClass() : null;
   }
}
