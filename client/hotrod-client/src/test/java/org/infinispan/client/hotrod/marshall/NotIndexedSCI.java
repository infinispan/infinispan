package org.infinispan.client.hotrod.marshall;

import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.NotIndexedMarshaller;
import org.infinispan.marshall.AbstractSerializationContextInitializer;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

public class NotIndexedSCI extends AbstractSerializationContextInitializer {

   public static final SerializationContextInitializer INSTANCE = new NotIndexedSCI();

   NotIndexedSCI() {
      super("not_indexed.proto");
   }

   @Override
   public String getProtoFile() {
      return "package sample_bank_account;\n" +
            "message NotIndexed {\n" +
            "\toptional string notIndexedField = 1;\n" +
            "}\n";
   }

   @Override
   public void registerMarshallers(SerializationContext ctx) {
      ctx.registerMarshaller(new NotIndexedMarshaller());
   }
}
