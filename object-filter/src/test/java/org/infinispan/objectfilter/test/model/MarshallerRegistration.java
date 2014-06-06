package org.infinispan.objectfilter.test.model;

import com.google.protobuf.Descriptors;
import org.infinispan.protostream.SerializationContext;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class MarshallerRegistration {

   public static final String PROTOBUF_RES = "/org/infinispan/objectfilter/test/model/test_model.protobin";

   public static void registerMarshallers(SerializationContext ctx) throws IOException, Descriptors.DescriptorValidationException {
      ctx.registerProtofile(MarshallerRegistration.class.getResourceAsStream(PROTOBUF_RES));
      ctx.registerMarshaller(new AddressMarshaller());
      ctx.registerMarshaller(new PhoneNumberMarshaller());
      ctx.registerMarshaller(new GenderMarshaller());
      ctx.registerMarshaller(new PersonMarshaller());
   }
}
