package org.infinispan.query.remote.client;

import com.google.protobuf.Descriptors;
import org.infinispan.protostream.SerializationContext;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class MarshallerRegistration {

   public static void registerMarshallers(SerializationContext ctx) throws IOException, Descriptors.DescriptorValidationException {
      ctx.registerProtofile(MarshallerRegistration.class.getResourceAsStream("/org/infinispan/query/remote/client/query.protobin"));
      ctx.registerMarshaller(new QueryRequestMarshaller());
      ctx.registerMarshaller(new QueryResponseMarshaller());
   }
}