package org.infinispan.client.hotrod.impl.query;

import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.impl.JPAQueryGenerator;

import java.util.Date;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class RemoteJPAQueryGenerator extends JPAQueryGenerator {

   private final SerializationContext serializationContext;

   public RemoteJPAQueryGenerator(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   //TODO [anistor] these are only used for remote query with Lucene engine

   @Override
   protected <E extends Enum<E>> String renderEnum(E argument) {
      EnumMarshaller<E> encoder = (EnumMarshaller<E>) serializationContext.getMarshaller(argument.getClass());
      return String.valueOf(encoder.encode(argument));
   }

   @Override
   protected String renderBoolean(boolean argument) {
      return argument ? "1" : "0";
   }

   @Override
   protected String renderDate(Date argument) {
      return Long.toString(argument.getTime());
   }
}
