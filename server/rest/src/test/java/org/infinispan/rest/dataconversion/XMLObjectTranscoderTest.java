package org.infinispan.rest.dataconversion;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = "functional", testName = "rest.XMLObjectTranscoderTest")
public class XMLObjectTranscoderTest extends AbstractTranscoderTest {
   protected Person dataSrc;

   @BeforeTest
   public void setUp() {
      dataSrc = new Person("Joe");
      Address address = new Address();
      address.setCity("London");
      dataSrc.setAddress(address);
      transcoder = new XMLObjectTranscoder();
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   @Override
   public void testTranscoderTranscode() {
      Object transcoded = transcoder.transcode(dataSrc, MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_XML);
      assertTrue(transcoded instanceof byte[], "Must be byte[]");

      String xmlString = new String((byte[]) transcoded);

      Object transcodedBack = transcoder.transcode(xmlString, MediaType.APPLICATION_XML, MediaType.APPLICATION_OBJECT);

      assertEquals(dataSrc, transcodedBack, "Must be an equal objects");

   }
}
