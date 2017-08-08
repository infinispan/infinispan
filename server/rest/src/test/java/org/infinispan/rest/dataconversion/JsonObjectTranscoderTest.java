package org.infinispan.rest.dataconversion;

import static org.testng.Assert.assertEquals;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.test.data.Person;
import org.testng.annotations.Test;

/**
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.JsonObjectTranscoderTest")
public class JsonObjectTranscoderTest {

   @Test
   public void testJsonObjectTranscoder() throws Exception {
      Person joe = new Person("joe");

      JsonObjectTranscoder transcoder = new JsonObjectTranscoder();

      MediaType jsonMediaType = MediaType.APPLICATION_JSON;
      MediaType personMediaType = MediaType.fromString("application/x-java-object;type=org.infinispan.test.data.Person");

      Object result = transcoder.transcode(joe, personMediaType, jsonMediaType);

      assertEquals(result, "{\"name\":\"joe\",\"address\":null}");

      Object fromJson = transcoder.transcode(result, jsonMediaType, personMediaType);

      assertEquals(fromJson, joe);
   }

}
