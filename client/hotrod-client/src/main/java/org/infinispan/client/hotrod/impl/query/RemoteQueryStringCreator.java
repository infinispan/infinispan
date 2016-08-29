package org.infinispan.client.hotrod.impl.query;

import java.util.Date;

import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.impl.QueryStringCreator;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class RemoteQueryStringCreator extends QueryStringCreator {

   private final SerializationContext serializationContext;

   public RemoteQueryStringCreator(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   //TODO [anistor] these are only used for remote query with Lucene engine

   @Override
   protected <E extends Enum<E>> String renderEnum(E argument) {
      EnumMarshaller<E> encoder = (EnumMarshaller<E>) serializationContext.getMarshaller(argument.getClass());
      return String.valueOf(encoder.encode(argument));
   }

   @Override
   protected String renderDate(Date argument) {
      return Long.toString(argument.getTime());
   }
}
