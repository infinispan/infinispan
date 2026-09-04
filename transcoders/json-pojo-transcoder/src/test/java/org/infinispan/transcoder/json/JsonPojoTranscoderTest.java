package org.infinispan.transcoder.json;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.util.Collections;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.util.Util;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "transcoder.json.JsonPojoTranscoderTest")
public class JsonPojoTranscoderTest extends AbstractTranscoderTest {

   @BeforeClass(alwaysRun = true)
   public void setUp() {
      transcoder = new JsonPojoTranscoder(new ClassAllowList(Collections.singletonList(".*")));
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   @Test
   public void testEmptyContent() {
      byte[] empty = Util.EMPTY_BYTE_ARRAY;
      assertArrayEquals(empty, (byte[]) transcoder.transcode(empty, APPLICATION_OBJECT, APPLICATION_JSON));
   }

   @Test
   public void testPojoToJson() {
      Person person = new Person("joe");
      Address address = new Address();
      address.setCity("London");
      person.setAddress(address);

      String json = (String) transcoder.transcode(person, APPLICATION_OBJECT, APPLICATION_JSON.withClassType(String.class));
      assertTrue(json.contains("\"joe\""));
      assertTrue(json.contains("\"London\""));
   }

   @Test
   public void testPojoToJsonBytes() {
      Person person = new Person("joe");
      Address address = new Address();
      address.setCity("London");
      person.setAddress(address);

      byte[] result = (byte[]) transcoder.transcode(person, APPLICATION_OBJECT, APPLICATION_JSON);
      String json = new String(result, UTF_8);
      assertTrue(json.contains("\"joe\""));
      assertTrue(json.contains("\"London\""));
   }
}
