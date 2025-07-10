package org.infinispan.rest.dataconversion;

import org.infinispan.commons.dataconversion.DefaultTranscoder;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.TextBinaryTranscoderTest")
public class TextBinaryTranscoderTest extends AbstractTranscoderTest {
   protected String dataSrc;

   @BeforeClass(alwaysRun = true)
   public void setUp() {
      dataSrc = " !\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
      transcoder = new DefaultTranscoder(new ProtoStreamMarshaller());
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

}
