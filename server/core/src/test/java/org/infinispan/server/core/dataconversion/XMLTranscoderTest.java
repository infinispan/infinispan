package org.infinispan.server.core.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.XMLTranscoderTest")
public class XMLTranscoderTest {

   private Person person;
   private XMLTranscoder xmlTranscoder = new XMLTranscoder(new ClassWhiteList(singletonList(".*")));

   @BeforeClass(alwaysRun = true)
   public void setUp() {
      person = new Person("Joe");
      Address address = new Address();
      address.setCity("London");
      person.setAddress(address);
   }

   public void testObjectToXML() {
      String xmlString = new String((byte[]) xmlTranscoder.transcode(person, APPLICATION_OBJECT, APPLICATION_XML));
      Object transcodedBack = xmlTranscoder.transcode(xmlString, APPLICATION_XML, APPLICATION_OBJECT);
      assertEquals("Must be an equal objects", person, transcodedBack);
   }

   public void testTextToXML() {
      byte[] value = "Hello World!".getBytes(UTF_8);

      Object asXML = xmlTranscoder.transcode(value, TEXT_PLAIN, APPLICATION_XML);
      assertEquals("<?xml version=\"1.0\" ?><string>Hello World!</string>", new String((byte[]) asXML));

      Object xmlAsText = xmlTranscoder.transcode(asXML, APPLICATION_XML, TEXT_PLAIN);
      assertArrayEquals("<?xml version=\"1.0\" ?><string>Hello World!</string>".getBytes(), (byte[]) xmlAsText);
   }
}
