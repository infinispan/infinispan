package org.infinispan.all.remote.sample.marshallers;

import org.infinispan.protostream.DescriptorParserException;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @author tsykora@redhat.com
 */
public class MarshallerRegistration {
   public static final String PROTOBUF_RES = "/bank.proto";
   public static void registerMarshallers(SerializationContext ctx) throws IOException, DescriptorParserException {
      ctx.registerProtoFiles(FileDescriptorSource.fromResources(PROTOBUF_RES));
      ctx.registerMarshaller(new UserMarshaller());
      ctx.registerMarshaller(new AddressMarshaller());
      ctx.registerMarshaller(new AccountMarshaller());
      ctx.registerMarshaller(new GenderMarshaller());
      ctx.registerMarshaller(new LimitsMarshaller());
      // Transaction Marshaller not needed, removed from sample for uber-jars (and bank.proto file)
   }
}