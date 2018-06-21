package org.infinispan.rest.dataconversion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.infinispan.commons.dataconversion.DefaultTranscoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.TextBinaryTranscoderTest")
public class TextBinaryTranscoderTest extends AbstractTranscoderTest {
   protected String dataSrc;

   @BeforeClass(alwaysRun = true)
   public void setUp() {
      dataSrc = " !\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
      transcoder = new DefaultTranscoder();
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   @Override
   public void testTranscoderTranscode() {
      Object transcoded = transcoder.transcode(dataSrc, MediaType.TEXT_PLAIN, MediaType.APPLICATION_OCTET_STREAM);
      assertTrue(transcoded instanceof byte[], "Must be byte[]");

      Object transcodedBack = transcoder.transcode(transcoded, MediaType.APPLICATION_OCTET_STREAM, MediaType.TEXT_PLAIN);
      assertEquals(transcodedBack, dataSrc.getBytes(), "Must be an equal objects");
   }
}
