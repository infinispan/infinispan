package org.infinispan.transcoder.xml;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_WWW_FORM_URLENCODED;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "transcoder.xml.XMLTranscoderTest")
public class XMLTranscoderTest {

   private Person person;
   private final XMLTranscoder xmlTranscoder = new XMLTranscoder(new ClassAllowList(singletonList(".*")));

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
      assertEquals(person, transcodedBack, "Must be an equal objects");
   }

   @Test
   public void testWWWFormUrlEncoded() {
      byte[] transcoded = (byte[]) xmlTranscoder.transcode("%3Cstring%3EHello%20World%21%3C%2Fstring%3E", APPLICATION_WWW_FORM_URLENCODED, APPLICATION_XML);
      assertEquals("<string>Hello World!</string>", new String(transcoded));
   }

   public void testXmlToOctetStream() {
      byte[] xml = "<root><key>value</key></root>".getBytes(UTF_8);
      byte[] result = (byte[]) xmlTranscoder.transcode(xml, APPLICATION_XML, APPLICATION_OCTET_STREAM);
      assertArrayEquals(xml, result);
   }

   public void testXmlToUnknown() {
      byte[] xml = "<root><key>value</key></root>".getBytes(UTF_8);
      byte[] result = (byte[]) xmlTranscoder.transcode(xml, APPLICATION_XML, APPLICATION_UNKNOWN);
      assertArrayEquals(xml, result);
   }

   public void testTextToXML() {
      byte[] value = "Hello World!".getBytes(UTF_8);

      Object asXML = xmlTranscoder.transcode(value, TEXT_PLAIN, APPLICATION_XML);
      assertEquals("<string>Hello World!</string>", new String((byte[]) asXML));

      Object xmlAsText = xmlTranscoder.transcode(asXML, APPLICATION_XML, TEXT_PLAIN);
      assertArrayEquals("<string>Hello World!</string>".getBytes(), (byte[]) xmlAsText);
   }
}
