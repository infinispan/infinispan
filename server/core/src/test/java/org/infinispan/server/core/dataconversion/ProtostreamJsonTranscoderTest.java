package org.infinispan.server.core.dataconversion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = "functional", testName = "query.remote.impl.ProtostreamJsonTranscoderTest")
public class ProtostreamJsonTranscoderTest extends AbstractTranscoderTest {

   private static final String PROTO_DEFINITIONS =
         "syntax = \"proto2\";\n" +
               "\n" +
               "    message Person {\n" +
               "    optional string _type = 1;\n" +
               "    optional string name = 2;\n" +
               "\n" +
               "    message Address {\n" +
               "      optional string _type = 1;\n" +
               "      optional string street = 2;\n" +
               "      optional string city = 3;\n" +
               "      optional string zip = 4;\n" +
               "    }\n" +
               "\n" +
               "    optional Address address = 3;\n" +
               "}";


   protected String dataSrc;

   @BeforeClass(alwaysRun = true)
   public void setUp() throws IOException {

      dataSrc = "{\"_type\":\"Person\", \"name\":\"joe\", \"address\":{\"_type\":\"Address\", \"street\":\"\", \"city\":\"London\", \"zip\":\"0\"}}";
      SerializationContext serCtx = ProtobufUtil.newSerializationContext();
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("person_definition.proto", PROTO_DEFINITIONS));
      transcoder = new ProtostreamJsonTranscoder(serCtx);
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   @Test
   @Override
   public void testTranscoderTranscode() throws Exception {
      Object transcoded = transcoder.transcode(dataSrc.getBytes(StandardCharsets.UTF_8), MediaType.APPLICATION_JSON, MediaType.APPLICATION_PROTOSTREAM);
      assertTrue(transcoded instanceof byte[], "Must be byte[]");

      Object transcodedBack = transcoder.transcode(transcoded, MediaType.APPLICATION_PROTOSTREAM, MediaType.APPLICATION_JSON);
      assertEquals(
            dataSrc.replace(" ", ""),
            (new String((byte[]) transcodedBack)).replace(" ", "").replace("\n", ""),
            "Must be the same JSON string"
      );
   }
}
