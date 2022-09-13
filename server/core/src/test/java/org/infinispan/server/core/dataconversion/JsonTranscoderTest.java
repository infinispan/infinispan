package org.infinispan.server.core.dataconversion;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.dataconversion.StandardConversions.convertCharset;
import static org.infinispan.server.core.dataconversion.JsonTranscoder.TYPE_PROPERTY;
import static org.testng.Assert.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.nio.charset.Charset;
import java.util.Collections;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
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
      transcoder = new JsonTranscoder(new ClassAllowList(Collections.singletonList(".*")));
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   @Override
   public void testTranscoderTranscode() throws Exception {
      MediaType jsonMediaType = APPLICATION_JSON;
      MediaType personMediaType = MediaType.fromString("application/x-java-object;type=org.infinispan.test.data.Person");

      Object result = transcoder.transcode(dataSrc, personMediaType, jsonMediaType);

      assertEquals(new String((byte[]) result),
            String.format("{\"" + TYPE_PROPERTY + "\":\"%s\",\"name\":\"%s\",\"address\":{\"" + TYPE_PROPERTY + "\":\"%s\",\"street\":null,\"city\":\"%s\",\"zip\":0},\"picture\":null,\"sex\":null,\"birthDate\":null,\"acceptedToS\":false,\"moneyOwned\":1.1,\"moneyOwed\":0.4,\"decimalField\":10.3,\"realField\":4.7}",
                  Person.class.getName(),
                  "joe",
                  Address.class.getName(),
                  "London")
      );

      Object fromJson = transcoder.transcode(result, jsonMediaType, personMediaType);

      assertEquals(fromJson, dataSrc);
   }

   @Test
   public void testEmptyContent() {
      byte[] empty = Util.EMPTY_BYTE_ARRAY;
      assertArrayEquals(empty, (byte[]) transcoder.transcode(empty, APPLICATION_JSON, APPLICATION_JSON.withCharset(US_ASCII)));
      assertArrayEquals(empty, (byte[]) transcoder.transcode(empty, APPLICATION_UNKNOWN, APPLICATION_JSON));
      assertArrayEquals(empty, (byte[]) transcoder.transcode(empty, APPLICATION_OCTET_STREAM, APPLICATION_JSON));
      assertArrayEquals(empty, (byte[]) transcoder.transcode(empty, TEXT_PLAIN, APPLICATION_JSON));
      assertArrayEquals(empty, (byte[]) transcoder.transcode(empty, APPLICATION_OBJECT, APPLICATION_JSON));
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

   private void assertTextToJsonConversion(String content) {
      final Object transcode = transcoder.transcode(content, TEXT_PLAIN, APPLICATION_JSON);
      assertArrayEquals((byte[]) transcode, content.getBytes(UTF_8));
   }

   @Test
   public void testTextToJson() {
      assertTextToJsonConversion("{\"a\":1}");
      assertTextToJsonConversion("1.5");
      assertTextToJsonConversion("\"test\"");
   }

   @Test(expectedExceptions = EncodingException.class)
   public void testPreventInvalidJson() {
      byte[] invalidContent = "\"field\" : value".getBytes(UTF_8);
      transcoder.transcode(invalidContent, TEXT_PLAIN, APPLICATION_JSON);
   }

   @Test(expectedExceptions = EncodingException.class)
   public void testPreventInvalidJson2() {
      byte[] invalidContent = "text".getBytes(UTF_8);
      transcoder.transcode(invalidContent, TEXT_PLAIN, APPLICATION_JSON);
   }

   @Test(expectedExceptions = EncodingException.class)
   public void testPreventInvalidJson3() {
      byte[] invalidContent = "{a:1}".getBytes(UTF_8);
      transcoder.transcode(invalidContent, TEXT_PLAIN, APPLICATION_JSON);
   }

}
