package org.infinispan.marshall.protostream.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;

import org.infinispan.marshall.protostream.impl.marshallers.UUIDMarshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "marshall.ProtoStreamBackwardsCompatibilityTest")
public class ProtoStreamBackwardsCompatibilityTest {

   public void testOldEventLoggerUUIDBytesAreReadable() throws IOException {
      // Initialize SerializationContext like it used to be in the server
      SerializationContext oldServerCtx = ProtobufUtil.newSerializationContext();
      PersistenceContextManualInitializer serverInitializer = new PersistenceContextManualInitializer();
      serverInitializer.registerSchema(oldServerCtx);
      serverInitializer.registerMarshallers(oldServerCtx);

      // Initialise SerializationContext using the new core initializer
      SerializationContext coreCtx = ProtobufUtil.newSerializationContext();
      org.infinispan.marshall.protostream.impl.PersistenceContextManualInitializer.INSTANCE.registerSchema(coreCtx);
      org.infinispan.marshall.protostream.impl.PersistenceContextManualInitializer.INSTANCE.registerMarshallers(coreCtx);

      UUID uuid = UUID.randomUUID();
      byte[] oldBytes = ProtobufUtil.toWrappedByteArray(oldServerCtx, uuid);
      UUID unmarshalled = ProtobufUtil.fromWrappedByteArray(coreCtx, oldBytes);
      assertEquals(uuid, unmarshalled);

      byte[] newBytes = ProtobufUtil.toWrappedByteArray(coreCtx, uuid);
      unmarshalled = ProtobufUtil.fromWrappedByteArray(coreCtx, newBytes);
      assertEquals(uuid, unmarshalled);
   }

   private static class PersistenceContextManualInitializer implements SerializationContextInitializer {

      String type(String message) {
         return String.format("org.infinispan.persistence.m.event_logger.%s", message);
      }

      private PersistenceContextManualInitializer() {}

      @Override
      public String getProtoFileName() {
         return "persistence.m.event_logger.proto";
      }

      @Override
      public String getProtoFile() throws UncheckedIOException {
         return "package org.infinispan.persistence.m.event_logger;\n" +
               "\n" +
               "/**\n" +
               " * @TypeId(1005)\n" +
               " * ProtoStreamTypeIds.SERVER_EVENT_UUID\n" +
               " */\n" +
               "message UUID {\n" +
               "    optional uint64 mostSigBits = 1;\n" +
               "    optional uint64 leastSigBits = 2;\n" +
               "}\n";
      }

      @Override
      public void registerSchema(SerializationContext serCtx) {
         serCtx.registerProtoFiles(org.infinispan.protostream.FileDescriptorSource.fromString(getProtoFileName(), getProtoFile()));
      }

      @Override
      public void registerMarshallers(SerializationContext serCtx) {
         serCtx.registerMarshaller(new OldUUIDMarshaller(type(UUID.class.getSimpleName())));
      }
   }

   private static class OldUUIDMarshaller extends UUIDMarshaller {

      public OldUUIDMarshaller(String typeName) {
         super(typeName);
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, UUID uuid) throws IOException {
         writer.writeLong("mostSigBits", uuid.getMostSignificantBits());
         writer.writeLong("leastSigBits", uuid.getLeastSignificantBits());
      }
   }
}
