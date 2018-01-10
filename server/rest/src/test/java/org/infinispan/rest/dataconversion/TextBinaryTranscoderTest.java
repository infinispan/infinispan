package org.infinispan.rest.dataconversion;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = "functional", testName = "rest.TextBinaryTranscoderTest")
public class TextBinaryTranscoderTest extends AbstractTranscoderTest {
   protected String dataSrc;

   @BeforeTest
   public void setUp() {
      dataSrc = " !\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
      transcoder = new TextBinaryTranscoder();
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   @Override
   public void testTranscoderTranscode() {

      Object transcoded = transcoder.transcode(dataSrc, MediaType.TEXT_PLAIN, MediaType.APPLICATION_OCTET_STREAM);

      assertTrue(transcoded instanceof byte[], "Must be byte[]");

      String transcodedBack = new String(
            Base64.getDecoder().decode(
                  (String) transcoder.transcode(
                        transcoded,
                        MediaType.APPLICATION_OCTET_STREAM,
                        MediaType.TEXT_PLAIN
                  )
            ),
            StandardCharsets.UTF_8
      );

      assertEquals(transcodedBack, dataSrc, "Must be an equal objects");
   }
}
