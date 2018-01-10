package org.infinispan.rest.dataconversion;

import static org.infinispan.rest.JSONConstants.TYPE;
import static org.testng.Assert.assertEquals;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.JsonObjectTranscoderTest")
public class JsonObjectTranscoderTest extends AbstractTranscoderTest {
   protected Person dataSrc;

   @BeforeTest
   public void setUp() {
      dataSrc = new Person("joe");
      Address address = new Address();
      address.setCity("London");
      dataSrc.setAddress(address);
      transcoder = new JsonObjectTranscoder();
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   @Override
   public void testTranscoderTranscode() throws Exception {
      MediaType jsonMediaType = MediaType.APPLICATION_JSON;
      MediaType personMediaType = MediaType.fromString("application/x-java-object;type=org.infinispan.test.data.Person");

      Object result = transcoder.transcode(dataSrc, personMediaType, jsonMediaType);

      assertEquals(result,
            String.format("{\"" + TYPE + "\":\"%s\",\"name\":\"%s\",\"address\":{\"" + TYPE + "\":\"%s\",\"street\":null,\"city\":\"%s\",\"zip\":0}}",
                  Person.class.getName(),
                  "joe",
                  Address.class.getName(),
                  "London")
      );

      Object fromJson = transcoder.transcode(result, jsonMediaType, personMediaType);

      assertEquals(fromJson, dataSrc);
   }

}
