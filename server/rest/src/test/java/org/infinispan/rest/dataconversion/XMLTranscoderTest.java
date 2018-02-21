package org.infinispan.rest.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.testng.Assert.assertEquals;

import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.XMLTranscoderTest")
public class XMLTranscoderTest extends AbstractTranscoderTest {
   protected Person dataSrc;

   @BeforeClass(alwaysRun = true)
   public void setUp() {
      dataSrc = new Person("Joe");
      Address address = new Address();
      address.setCity("London");
      dataSrc.setAddress(address);
      transcoder = new XMLTranscoder();
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   @Override
   public void testTranscoderTranscode() {
      String xmlString = (String) transcoder.transcode(dataSrc, APPLICATION_OBJECT, APPLICATION_XML);

      Object transcodedBack = transcoder.transcode(xmlString, APPLICATION_XML, APPLICATION_OBJECT);

      assertEquals(dataSrc, transcodedBack, "Must be an equal objects");

   }
}
