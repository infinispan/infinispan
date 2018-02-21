package org.infinispan.query.remote.impl.dataconversion;


import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = "functional", testName = "query.remote.impl.ProtostreamTextTranscoderTest")
public class ProtostreamTextTranscoderTest extends AbstractTranscoderTest {

   protected String dataSrc;

   @BeforeClass(alwaysRun = true)
   public void setUp() throws IOException {
      dataSrc = " !\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
      SerializationContext serCtx = ProtobufUtil.newSerializationContext(Configuration.builder().build());
      transcoder = new ProtostreamTextTranscoder(serCtx);
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   @Test
   @Override
   public void testTranscoderTranscode() throws Exception {
      Object transcoded = transcoder.transcode(dataSrc, MediaType.TEXT_PLAIN, MediaType.APPLICATION_PROTOSTREAM);
      assertTrue(transcoded instanceof byte[], "Must be byte[]");

      Object transcodedBack = transcoder.transcode(transcoded, MediaType.APPLICATION_PROTOSTREAM, MediaType.TEXT_PLAIN);

      assertTrue(transcodedBack instanceof String, "Must be instance of String");
      assertEquals(dataSrc, transcodedBack, "Must be equal strings");
   }
}
