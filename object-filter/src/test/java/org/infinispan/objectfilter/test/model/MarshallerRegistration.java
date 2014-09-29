package org.infinispan.objectfilter.test.model;

import org.infinispan.protostream.DescriptorParserException;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class MarshallerRegistration {

   public static final String PROTOBUF_RES = "/org/infinispan/objectfilter/test/model/test_model.proto";

   public static void registerMarshallers(SerializationContext ctx) throws IOException, DescriptorParserException {
      ctx.registerProtoFiles(FileDescriptorSource.fromResources(PROTOBUF_RES));
      ctx.registerMarshaller(new AddressMarshaller());
      ctx.registerMarshaller(new PhoneNumberMarshaller());
      ctx.registerMarshaller(new GenderMarshaller());
      ctx.registerMarshaller(new PersonMarshaller());
   }
}
