package org.infinispan.query.remote.client.impl;

import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * Registers protobuf schemas and marshallers for the objects used by remote query, remote continuous query and Ickle
 * based filters.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class MarshallerRegistration implements SerializationContextInitializer {

   private static final String QUERY_PROTO_RES = "/org/infinispan/query/remote/client/query.proto";

   public static final MarshallerRegistration INSTANCE = new MarshallerRegistration();

   private MarshallerRegistration() {
   }

   @Override
   public String getProtoFileName() {
      return QUERY_PROTO_RES;
   }

   @Override
   public String getProtoFile() {
      return FileDescriptorSource.getResourceAsString(getClass(), QUERY_PROTO_RES);
   }

   @Override
   public void registerSchema(SerializationContext serCtx) {
      serCtx.registerProtoFiles(FileDescriptorSource.fromString(getProtoFileName(), getProtoFile()));
   }

   @Override
   public void registerMarshallers(SerializationContext ctx) {
      ctx.registerMarshaller(new QueryRequest.NamedParameter.Marshaller());
      ctx.registerMarshaller(new QueryRequest.Marshaller());
      ctx.registerMarshaller(new QueryResponse.Marshaller());
      ctx.registerMarshaller(new FilterResultMarshaller());
      ctx.registerMarshaller(new ContinuousQueryResult.ResultType.Marshaller());
      ctx.registerMarshaller(new ContinuousQueryResult.Marshaller());
   }

   /**
    * Registers proto files and marshallers.
    *
    * @param ctx the serialization context
    */
   public static void init(SerializationContext ctx) {
      INSTANCE.registerSchema(ctx);
      INSTANCE.registerMarshallers(ctx);
   }
}
