package org.infinispan.rest.dataconversion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.infinispan.commons.dataconversion.DefaultTranscoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = "functional", testName = "rest.TextObjectTranscoderTest")
public class TextObjectTranscoderTest extends AbstractTranscoderTest {
   protected Person dataSrc;

   @BeforeClass(alwaysRun = true)
   public void setUp() {
      dataSrc = new Person("Joe");
      Address address = new Address();
      address.setCity("London");
      dataSrc.setAddress(address);
      transcoder = DefaultTranscoder.INSTANCE;
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   @Override
   public void testTranscoderTranscode() {
      Object transcoded = transcoder.transcode(dataSrc, MediaType.APPLICATION_OBJECT, MediaType.TEXT_PLAIN);

      assertEquals(new String((byte[]) transcoded), dataSrc.toString());

      transcoded = transcoder.transcode(transcoded, MediaType.APPLICATION_OBJECT, MediaType.TEXT_PLAIN);

      assertTrue(transcoded instanceof byte[], "Must be byte[]");
   }
}
