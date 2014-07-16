package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.protostream.SerializationContext;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ProtobufEntityNamesResolver implements EntityNamesResolver {

   private final SerializationContext serializationContext;

   public ProtobufEntityNamesResolver(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   @Override
   public Class<?> getClassFromName(String entityName) {
      // The EntityNamesResolver of the HQL parser is not nicely designed to handle non-Class type metadata.
      // Here we return a 'fake' class. It does not matter what we return as long as it is non-null.
      return serializationContext.canMarshall(entityName) ? Object.class : null;
   }
}
