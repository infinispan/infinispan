package org.infinispan.commons.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_WWW_FORM_URLENCODED;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.net.URLEncoder;
import java.util.Base64;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.util.KeyValueWithPrevious;
import org.junit.BeforeClass;
import org.junit.Test;

public class BinaryTranscoderTest {

   private static BinaryTranscoder binaryTranscoder;
   private static JavaSerializationMarshaller marshaller;

   @BeforeClass
   public static void prepare() {
      final ClassAllowList allowList = new ClassAllowList();
      allowList.addRegexps(".*");
      marshaller = new JavaSerializationMarshaller(allowList);
      binaryTranscoder = new BinaryTranscoder(marshaller);
   }

   @Test
   public void testToFromUnknown() {
      testWithContentTypes(APPLICATION_UNKNOWN, APPLICATION_UNKNOWN);
   }

   @Test
   public void testToFromOctetStream() {
      testWithContentTypes(APPLICATION_UNKNOWN, APPLICATION_OCTET_STREAM);
      testWithContentTypes(APPLICATION_OCTET_STREAM, APPLICATION_UNKNOWN);
   }

   @Test
   public void testToFromTextPlain() {
      testWithContentTypes(APPLICATION_UNKNOWN, TEXT_PLAIN);
      testWithContentTypes(TEXT_PLAIN, APPLICATION_UNKNOWN);
   }

   @Test
   public void testToFromUrlEncoded() throws Exception {
      String data = "word1 word2";
      byte[] dataBinary = data.getBytes(UTF_8);
      final String encoded = URLEncoder.encode(data, "utf-8");
      final byte[] dataEncoded = encoded.getBytes(UTF_8);

      Object toBinary = binaryTranscoder.transcode(dataEncoded, APPLICATION_WWW_FORM_URLENCODED, APPLICATION_UNKNOWN);
      Object toBinaryHex = binaryTranscoder.transcode(dataEncoded, APPLICATION_WWW_FORM_URLENCODED, hexEncoded(APPLICATION_UNKNOWN));
      Object toBinaryBase64 = binaryTranscoder.transcode(dataEncoded, APPLICATION_WWW_FORM_URLENCODED, base64Encoded(APPLICATION_UNKNOWN));

      assertArrayEquals(dataBinary, (byte[]) toBinary);
      assertEquals(Base16Codec.encode(dataBinary), toBinaryHex);
      assertSame(Base64.getEncoder().encode(dataBinary), toBinaryBase64);

      Object fromBinary = binaryTranscoder.transcode(toBinary, APPLICATION_UNKNOWN, APPLICATION_WWW_FORM_URLENCODED);
      Object fromBinaryHex = binaryTranscoder.transcode(toBinaryHex, hexEncoded(APPLICATION_UNKNOWN), APPLICATION_WWW_FORM_URLENCODED);
      Object fromBinaryBase64 = binaryTranscoder.transcode(toBinaryBase64, base64Encoded(APPLICATION_UNKNOWN), APPLICATION_WWW_FORM_URLENCODED);

      assertSame(dataEncoded, fromBinary);
      assertSame(dataEncoded, fromBinaryHex);
      assertSame(dataEncoded, fromBinaryBase64);
   }

   @Test
   public void testToFromObject() throws Exception {
      testToFromObject("string-data");
      testToFromObject(new KeyValueWithPrevious<>("string", 1L, 0L));
      testToFromObject(new byte[]{1, 2, 3});
   }

   private void testToFromObject(Object data) throws Exception {
      byte[] binaryForm = marshaller.objectToByteBuffer(data);

      Object toBinary = binaryTranscoder.transcode(data, APPLICATION_OBJECT, APPLICATION_UNKNOWN);
      Object toBinaryHex = binaryTranscoder.transcode(data, APPLICATION_OBJECT, hexEncoded(APPLICATION_UNKNOWN));
      Object toBinaryBase64 = binaryTranscoder.transcode(data, APPLICATION_OBJECT, base64Encoded(APPLICATION_UNKNOWN));

      assertArrayEquals(binaryForm, (byte[]) toBinary);
      assertEquals(Base16Codec.encode(binaryForm), toBinaryHex);
      assertSame(Base64.getEncoder().encode(binaryForm), toBinaryBase64);

      Object fromBinary = binaryTranscoder.transcode(toBinary, APPLICATION_UNKNOWN, APPLICATION_OBJECT);
      Object fromBinaryHex = binaryTranscoder.transcode(toBinaryHex, hexEncoded(APPLICATION_UNKNOWN), APPLICATION_OBJECT);
      Object fromBinaryBase64 = binaryTranscoder.transcode(toBinaryBase64, base64Encoded(APPLICATION_UNKNOWN), APPLICATION_OBJECT);

      assertSame(data, fromBinary);
      assertSame(data, fromBinaryHex);
      assertSame(data, fromBinaryBase64);
   }

   private void assertSame(Object expected, Object obtained) {
      if (expected instanceof byte[]) {
         assertArrayEquals((byte[]) expected, (byte[]) obtained);
      } else {
         assertEquals(expected, obtained);
      }
   }

   private void testWithContentTypes(MediaType sourceType, MediaType targetType) {
      byte[] data = {1, 2, 3};
      MediaType[] sourceTypes = {sourceType, hexEncoded(sourceType), base64Encoded(sourceType)};
      MediaType[] targetTypes = {targetType, hexEncoded(targetType), base64Encoded(targetType)};

      for (MediaType source : sourceTypes) {
         for (MediaType target : targetTypes) {
            Object dataIn = encodeIfNeeded(data, source);
            Object expected = encodeIfNeeded(data, target);
            Object result = binaryTranscoder.transcode(dataIn, source, target);
            assertSame(expected, result);
         }
      }
   }

   private MediaType hexEncoded(MediaType mediaType) {
      return mediaType.withParameter("encoding", "hex");
   }

   private MediaType base64Encoded(MediaType mediaType) {
      return mediaType.withParameter("encoding", "base64");
   }

   private Object encodeIfNeeded(Object data, MediaType type) {
      final String encoding = type.getEncoding();

      if (encoding == null || (!(data instanceof byte[]))) return data;

      return new RFC4648Codec().encodeContent(data, type);
   }
}
