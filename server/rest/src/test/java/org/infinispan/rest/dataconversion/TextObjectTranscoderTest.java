package org.infinispan.rest.dataconversion;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


@Test(groups = "functional", testName = "rest.TextObjectTranscoderTest")
public class TextObjectTranscoderTest extends AbstractTranscoderTest {
   protected Person dataSrc;

   @BeforeTest
   public void setUp() {
      dataSrc = new Person("Joe");
      Address address = new Address();
      address.setCity("London");
      dataSrc.setAddress(address);
      transcoder = new TextObjectTranscoder();
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   @Override
   public void testTranscoderTranscode() throws Exception {
      Object transcoded = transcoder.transcode(dataSrc, MediaType.APPLICATION_OBJECT, MediaType.TEXT_PLAIN);

      assertTrue(transcoded instanceof String, "Must be String");

      transcoded = transcoder.transcode(((String) transcoded).getBytes("UTF-8"), MediaType.APPLICATION_OBJECT, MediaType.TEXT_PLAIN);

      assertTrue(transcoded instanceof byte[], "Must be byte[]");
   }
}
