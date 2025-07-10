package org.infinispan.dataconversion;

import org.infinispan.encoding.ProtostreamTranscoder;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = "functional", testName = "query.remote.impl.ProtostreamJsonTranscoderTest")
public class ProtostreamJsonTranscoderTest extends AbstractTranscoderTest {

   private static final String PROTO_DEFINITIONS =
         """
               syntax = "proto2";

                   message Person {
                   optional string _type = 1;
                   optional string name = 2;

                   message Address {
                     optional string _type = 1;
                     optional string street = 2;
                     optional string city = 3;
                     optional string zip = 4;
                   }

                   optional Address address = 3;
               }""";

   protected String dataSrc;

   @BeforeClass(alwaysRun = true)
   public void setUp() {
      dataSrc = """
            {"_type":"Person", "name":"joe", "address":{"_type":"Address", "street":"", "city":"London", "zip":"0"}}""";
      SerializationContext serCtx = ProtobufUtil.newSerializationContext();
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("person_definition.proto", PROTO_DEFINITIONS));
      SerializationContextRegistry registry = Mockito.mock(SerializationContextRegistry.class);
      Mockito.when(registry.getUserCtx()).thenReturn(serCtx);
      transcoder = new ProtostreamTranscoder(registry, ProtostreamTranscoder.class.getClassLoader());
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

}
