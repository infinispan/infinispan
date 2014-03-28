package org.infinispan.client.hotrod.impl.query;

import org.infinispan.protostream.EnumMarshaller;
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
   protected String renderEntityName(String rootType) {
      // this just checks the type can actually be marshalled with current config
      serializationContext.getMarshaller(rootType);

      return rootType;
   }

   @Override
   protected <E extends Enum<E>> String renderEnum(E argument) {
      EnumMarshaller<E> encoder = (EnumMarshaller<E>) serializationContext.getMarshaller(argument.getClass());
      return String.valueOf(encoder.encode(argument));
   }

   @Override
   protected String renderBoolean(boolean argument) {
      return argument ? "1" : "0";
   }
}
