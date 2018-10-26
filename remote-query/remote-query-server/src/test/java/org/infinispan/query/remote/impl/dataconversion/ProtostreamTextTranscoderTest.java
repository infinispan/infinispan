package org.infinispan.query.remote.impl.dataconversion;


import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.remote.impl.ProtostreamTextTranscoderTest")
public class ProtostreamTextTranscoderTest extends AbstractTranscoderTest {

   protected String dataSrc;

   @BeforeClass(alwaysRun = true)
   public void setUp() throws IOException {
      dataSrc = " !\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
      transcoder = new ProtostreamTextTranscoder(ProtobufUtil.newSerializationContext());
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
