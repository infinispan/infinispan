package org.infinispan.query.remote.client;

import java.io.IOException;

import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;

/**
 * @author anistor@redhat.com
 * @since 6.0
 * @private
 */
public final class MarshallerRegistration {

   public static final String QUERY_PROTO_RES = "/org/infinispan/query/remote/client/query.proto";
   public static final String MESSAGE_PROTO_RES = "/org/infinispan/protostream/message-wrapping.proto";

   private MarshallerRegistration() {
   }

   /**
    * Registers proto files and marshallers.
    *
    * @param ctx the serialization context
    * @throws org.infinispan.protostream.DescriptorParserException if a proto definition file fails to parse correctly
    * @throws IOException if proto file registration fails
    */
   public static void registerMarshallers(SerializationContext ctx) throws IOException {
      FileDescriptorSource fileDescriptorSource = new FileDescriptorSource();
      fileDescriptorSource.addProtoFile(QUERY_PROTO_RES, MarshallerRegistration.class.getResourceAsStream(QUERY_PROTO_RES));
      fileDescriptorSource.addProtoFile(MESSAGE_PROTO_RES, MarshallerRegistration.class.getResourceAsStream(MESSAGE_PROTO_RES));
      ctx.registerProtoFiles(fileDescriptorSource);
      ctx.registerMarshaller(new QueryRequest.NamedParameter.Marshaller());
      ctx.registerMarshaller(new QueryRequest.Marshaller());
      ctx.registerMarshaller(new QueryResponse.Marshaller());
      ctx.registerMarshaller(new FilterResult.Marshaller());
      ctx.registerMarshaller(new ContinuousQueryResult.ResultType.Marshaller());
      ctx.registerMarshaller(new ContinuousQueryResult.Marshaller());
   }
}
