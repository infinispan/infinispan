package org.infinispan.query.remote.client;

import com.google.protobuf.Descriptors;
import org.infinispan.protostream.SerializationContext;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class MarshallerRegistration {

   public static final String PROTOBUF_RES = "/org/infinispan/query/remote/client/query.protobin";

   public static void registerMarshallers(SerializationContext ctx) throws IOException, Descriptors.DescriptorValidationException {
      ctx.registerProtofile(MarshallerRegistration.class.getResourceAsStream(PROTOBUF_RES));
      ctx.registerMarshaller(new QueryRequestMarshaller());
      ctx.registerMarshaller(new QueryResponseMarshaller());
   }
}