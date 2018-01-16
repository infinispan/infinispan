package org.infinispan.rest.dataconversion;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = "functional", testName = "rest.JavaSerializationTranscoderTest")
public class JavaSerializationTranscoderTest extends AbstractTranscoderTest {
   protected Person dataSrc;

   @BeforeTest
   public void setUp() {
      dataSrc = new Person("Joe");
      Address address = new Address();
      address.setCity("London");
      dataSrc.setAddress(address);
      transcoder = new JavaSerializationTranscoder();
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   @Override
   public void testTranscoderTranscode() {
      MediaType personMediaType = MediaType.fromString("application/x-java-object;type=org.infinispan.test.data.Person");
      Object result = transcoder.transcode(dataSrc, personMediaType, MediaType.APPLICATION_SERIALIZED_OBJECT);

      assertTrue(result instanceof byte[], "Must be byte[]");

      Object transcodedBack = transcoder.transcode(result, MediaType.APPLICATION_SERIALIZED_OBJECT, personMediaType);

      assertEquals(transcodedBack, dataSrc, "Must be an equal objects");
   }
}
