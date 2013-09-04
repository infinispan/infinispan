package org.infinispan.client.hotrod.impl.query;

import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.impl.JPAQueryGenerator;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class RemoteJPAQueryGenerator extends JPAQueryGenerator {

   private final SerializationContext serCtx;

   public RemoteJPAQueryGenerator(SerializationContext serCtx) {
      this.serCtx = serCtx;
   }

   @Override
   protected String getIndexedEntityName(Class<?> rootType) {
      return serCtx.getMarshaller(rootType).getFullName();
   }
}
