package org.infinispan.client.hotrod.impl.query;

import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.impl.JPAQueryGenerator;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class RemoteJPAQueryGenerator extends JPAQueryGenerator {

   private final SerializationContext serializationContext;

   public RemoteJPAQueryGenerator(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   @Override
   protected String getIndexedEntityName(Class<?> rootType) {
      return serializationContext.getMarshaller(rootType).getTypeName();
   }
}
