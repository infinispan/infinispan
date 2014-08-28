package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import org.infinispan.protostream.DescriptorParserException;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class MarshallerRegistration {

   public static final String PROTOBUF_RES = "/sample_bank_account/bank.proto";

   public static void registerMarshallers(SerializationContext ctx) throws IOException, DescriptorParserException {
      ctx.registerProtoFiles(FileDescriptorSource.fromResources(PROTOBUF_RES));
      ctx.registerMarshaller(new UserMarshaller());
      ctx.registerMarshaller(new GenderMarshaller());
      ctx.registerMarshaller(new AddressMarshaller());
      ctx.registerMarshaller(new AccountMarshaller());
      ctx.registerMarshaller(new TransactionMarshaller());
   }
}
