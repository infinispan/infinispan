package org.infinispan.server.core.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.dataconversion.StandardConversions.convertCharset;
import static org.infinispan.server.core.dataconversion.JsonTranscoder.TYPE_PROPERTY;
import static org.testng.Assert.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.nio.charset.Charset;
import java.util.Collections;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @since 9.2
 */
@Test(groups = "functional", testName = "server.core.dataconversion.JsonTranscoderTest")
public class JsonTranscoderTest extends AbstractTranscoderTest {
   protected Person dataSrc;

   @BeforeClass(alwaysRun = true)
   public void setUp() {
      dataSrc = new Person("joe");
      Address address = new Address();
      address.setCity("London");
      dataSrc.setAddress(address);
      transcoder = new JsonTranscoder(new ClassWhiteList(Collections.singletonList(".*")));
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   @Override
   public void testTranscoderTranscode() throws Exception {
      MediaType jsonMediaType = APPLICATION_JSON;
      MediaType personMediaType = MediaType.fromString("application/x-java-object;type=org.infinispan.test.data.Person");

      Object result = transcoder.transcode(dataSrc, personMediaType, jsonMediaType);

      assertEquals(new String((byte[]) result),
            String.format("{\"" + TYPE_PROPERTY + "\":\"%s\",\"name\":\"%s\",\"address\":{\"" + TYPE_PROPERTY + "\":\"%s\",\"street\":null,\"city\":\"%s\",\"zip\":0}}",
                  Person.class.getName(),
                  "joe",
                  Address.class.getName(),
                  "London")
      );

      Object fromJson = transcoder.transcode(result, jsonMediaType, personMediaType);

      assertEquals(fromJson, dataSrc);
   }

   @Test
   public void testCharset() {
      Charset korean = Charset.forName("EUC-KR");
      MediaType textPlainKorean = TEXT_PLAIN.withCharset(korean);
      MediaType jsonKorean = APPLICATION_JSON.withCharset(korean);
      MediaType textPlainAsString = TEXT_PLAIN.withClassType(String.class);
      MediaType jsonAsString = APPLICATION_JSON.withClassType(String.class);

      String content = "{\"city_kr\":\"서울\"}";
      byte[] contentUTF = content.getBytes(UTF_8);
      byte[] contentKorean = convertCharset(contentUTF, UTF_8, korean);

      byte[] result = (byte[]) transcoder.transcode(contentKorean, jsonKorean, TEXT_PLAIN);
      assertArrayEquals(result, contentUTF);

      String strResult = (String) transcoder.transcode(contentKorean, jsonKorean, textPlainAsString);
      assertEquals(strResult, content);

      result = (byte[]) transcoder.transcode(contentKorean, jsonKorean, textPlainKorean);
      assertArrayEquals(result, contentKorean);

      result = (byte[]) transcoder.transcode(contentKorean, textPlainKorean, APPLICATION_JSON);
      assertArrayEquals(result, contentUTF);

      strResult = (String) transcoder.transcode(contentKorean, jsonKorean, jsonAsString);
      assertEquals(strResult, content);

      result = (byte[]) transcoder.transcode(contentKorean, textPlainKorean, jsonKorean);
      assertArrayEquals(result, contentKorean);

      result = (byte[]) transcoder.transcode(contentUTF, TEXT_PLAIN, jsonKorean);
      assertArrayEquals(result, contentKorean);

      result = (byte[]) transcoder.transcode(content, textPlainAsString, jsonKorean);
      assertArrayEquals(result, contentKorean);

      result = (byte[]) transcoder.transcode(contentUTF, APPLICATION_JSON, TEXT_PLAIN);
      assertArrayEquals(result, contentUTF);

      result = (byte[]) transcoder.transcode(contentUTF, APPLICATION_JSON, textPlainKorean);
      assertArrayEquals(result, contentKorean);
   }
}
