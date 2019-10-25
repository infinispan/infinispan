package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

public class TestDomainSCI implements SerializationContextInitializer {
   static final String PROTOBUF_RES = "sample_bank_account/bank.proto";
   public static final TestDomainSCI INSTANCE = new TestDomainSCI();

   private TestDomainSCI() {
   }

   @Override
   public String getProtoFileName() {
      return PROTOBUF_RES;
   }

   @Override
   public String getProtoFile() {
      return FileDescriptorSource.getResourceAsString(getClass(), "/" + PROTOBUF_RES);
   }

   @Override
   public void registerSchema(SerializationContext serCtx) {
      serCtx.registerProtoFiles(FileDescriptorSource.fromString(getProtoFileName(), getProtoFile()));
   }

   @Override
   public void registerMarshallers(SerializationContext ctx) {
      ctx.registerMarshaller(new UserMarshaller());
      ctx.registerMarshaller(new GenderMarshaller());
      ctx.registerMarshaller(new AddressMarshaller());
      ctx.registerMarshaller(new AccountMarshaller());
      ctx.registerMarshaller(new CurrencyMarshaller());
      ctx.registerMarshaller(new LimitsMarshaller());
      ctx.registerMarshaller(new TransactionMarshaller());
   }
}
