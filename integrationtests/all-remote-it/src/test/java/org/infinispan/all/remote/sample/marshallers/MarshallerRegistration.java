package org.infinispan.all.remote.sample.marshallers;

import java.io.IOException;

import org.infinispan.protostream.DescriptorParserException;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class MarshallerRegistration {

   public static final String PROTOBUF_RES = "/sample_bank_account/bank.proto";

   public static void registerMarshallers(SerializationContext ctx) throws IOException, DescriptorParserException {
      ctx.registerProtoFiles(FileDescriptorSource.fromResources(PROTOBUF_RES));
      ctx.registerMarshaller(new UserMarshaller());
      ctx.registerMarshaller(new GenderMarshaller());
      ctx.registerMarshaller(new AddressMarshaller());
      ctx.registerMarshaller(new AccountMarshaller());
   }
}
