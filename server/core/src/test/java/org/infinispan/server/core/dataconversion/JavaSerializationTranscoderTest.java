package org.infinispan.server.core.dataconversion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Collections;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.JavaSerializationTranscoderTest")
public class JavaSerializationTranscoderTest extends AbstractTranscoderTest {
   protected Person dataSrc;

   @BeforeClass(alwaysRun = true)
   public void setUp() {
      dataSrc = new Person("Joe");
      Address address = new Address();
      address.setCity("London");
      dataSrc.setAddress(address);
      transcoder = new JavaSerializationTranscoder(new ClassWhiteList(Collections.singletonList(".*")));
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
