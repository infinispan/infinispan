package org.infinispan.commons.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_WWW_FORM_URLENCODED;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Base64;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.util.KeyValueWithPrevious;
import org.junit.BeforeClass;
import org.junit.Test;

public class DefaultTranscoderTest {

   private static DefaultTranscoder defaultTranscoder;
   private static JavaSerializationMarshaller marshaller;

   @BeforeClass
   public static void prepare() {
      final ClassAllowList allowList = new ClassAllowList();
      allowList.addRegexps(".*");
      marshaller = new JavaSerializationMarshaller(allowList);
      defaultTranscoder = new DefaultTranscoder(marshaller);
   }

   @Test
   public void testObjectOctetStream() throws Exception {
      final String string = "string-data";
      final byte[] byteArray = {1, 2, 3};
      final KeyValueWithPrevious<String, Long> pojo = new KeyValueWithPrevious<>("string", 1L, 0L);

      testObjectOctetStream(string, string.getBytes(UTF_8));
      testObjectOctetStream(byteArray, byteArray);
      testObjectOctetStream(pojo, marshaller.objectToByteBuffer(pojo));
   }

   private void testObjectOctetStream(Object data, byte[] expectOctetStream) {
      Object toOctetStream = defaultTranscoder.transcode(data, APPLICATION_OBJECT, APPLICATION_OCTET_STREAM);
      Object toOctetStreamHex = defaultTranscoder.transcode(data, APPLICATION_OBJECT, hexEncoded(APPLICATION_OCTET_STREAM));
      Object toOctetStreamBase64 = defaultTranscoder.transcode(data, APPLICATION_OBJECT, base64Encoded(APPLICATION_OCTET_STREAM));

      assertSame(expectOctetStream, toOctetStream);
      assertSame(Base16Codec.encode(expectOctetStream), toOctetStreamHex);
      assertSame(Base64.getEncoder().encode(expectOctetStream), toOctetStreamBase64);

      Object fromOctetStream = defaultTranscoder.transcode(toOctetStream, APPLICATION_OCTET_STREAM, APPLICATION_OBJECT);
      Object fromOctetStreamHex = defaultTranscoder.transcode(toOctetStreamHex, hexEncoded(APPLICATION_OCTET_STREAM), APPLICATION_OBJECT);
      Object fromOctetStreamBase64 = defaultTranscoder.transcode(toOctetStreamBase64, base64Encoded(APPLICATION_OCTET_STREAM), APPLICATION_OBJECT);

      assertSame(expectOctetStream, fromOctetStream);
      assertSame(expectOctetStream, fromOctetStreamHex);
      assertSame(expectOctetStream, fromOctetStreamBase64);
   }

   @Test
   public void testObjectUrlEncoded() throws Exception {
      String data = "word1 word2";
      final String encoded = URLEncoder.encode(data, "utf-8");
      final byte[] encodedUTF = encoded.getBytes(UTF_8);

      Object toObject = defaultTranscoder.transcode(encodedUTF, APPLICATION_WWW_FORM_URLENCODED, APPLICATION_OBJECT);
      assertEquals(data, toObject);

      Object fromObject = defaultTranscoder.transcode(toObject, APPLICATION_OBJECT, APPLICATION_WWW_FORM_URLENCODED);

      assertSame(encodedUTF, fromObject);
   }

   @Test
   public void testObjectText() throws Exception {
      String textData = "this is text";
      byte[] utf8 = textData.getBytes(UTF_8);

      Object textPlain = defaultTranscoder.transcode(textData, APPLICATION_OBJECT, TEXT_PLAIN);
      assertSame(utf8, textPlain);

      Object object = defaultTranscoder.transcode(textPlain, TEXT_PLAIN, APPLICATION_OBJECT.withClassType(String.class));
      assertEquals(textData, object);

      Object fromText = defaultTranscoder.transcode(textPlain, TEXT_PLAIN, APPLICATION_OBJECT);
      assertSame(textData, fromText);
   }

   @Test
   public void testOctetStreamUrlEncoded() throws UnsupportedEncodingException {
      String data = "word1 word2";
      byte[] urlEncoded = URLEncoder.encode(data, "utf-8").getBytes(UTF_8);
      byte[] utf8 = data.getBytes(UTF_8);

      Object toOctetStream = defaultTranscoder.transcode(urlEncoded, APPLICATION_WWW_FORM_URLENCODED, APPLICATION_OCTET_STREAM);
      Object toOctetStreamHex = defaultTranscoder.transcode(data, APPLICATION_WWW_FORM_URLENCODED, hexEncoded(APPLICATION_OCTET_STREAM));
      Object toOctetStream64 = defaultTranscoder.transcode(data, APPLICATION_WWW_FORM_URLENCODED, base64Encoded(APPLICATION_OCTET_STREAM));

      assertSame(utf8, toOctetStream);
      assertSame(Base16Codec.encode(utf8), toOctetStreamHex);
      assertSame(Base64.getEncoder().encode(utf8), toOctetStream64);

      Object fromOctetStream = defaultTranscoder.transcode(toOctetStream, APPLICATION_OCTET_STREAM, APPLICATION_WWW_FORM_URLENCODED);
      Object fromOctetStreamHex = defaultTranscoder.transcode(toOctetStreamHex, hexEncoded(APPLICATION_OCTET_STREAM), APPLICATION_WWW_FORM_URLENCODED);
      Object fromOctetStream64 = defaultTranscoder.transcode(toOctetStream64, base64Encoded(APPLICATION_OCTET_STREAM), APPLICATION_WWW_FORM_URLENCODED);

      assertSame(urlEncoded, fromOctetStream);
      assertSame(urlEncoded, fromOctetStreamHex);
      assertSame(urlEncoded, fromOctetStream64);
   }

   @Test
   public void testOctetStreamText() {
      String text = "word1 word2";
      byte[] utf8 = text.getBytes(UTF_8);

      Object toOctetStream = defaultTranscoder.transcode(text, TEXT_PLAIN, APPLICATION_OCTET_STREAM);
      Object toOctetStreamHex = defaultTranscoder.transcode(text, TEXT_PLAIN, hexEncoded(APPLICATION_OCTET_STREAM));
      Object toOctetStream64 = defaultTranscoder.transcode(text, TEXT_PLAIN, base64Encoded(APPLICATION_OCTET_STREAM));

      assertSame(utf8, toOctetStream);
      assertEquals(Base16Codec.encode(utf8), toOctetStreamHex);
      assertSame(Base64.getEncoder().encode(utf8), toOctetStream64);

      Object toOctetStreamFromByteArray = defaultTranscoder.transcode(utf8, TEXT_PLAIN, APPLICATION_OCTET_STREAM);
      Object toOctetStreamHexFromByteArray = defaultTranscoder.transcode(utf8, TEXT_PLAIN, hexEncoded(APPLICATION_OCTET_STREAM));
      Object toOctetStream64FromByteArray = defaultTranscoder.transcode(utf8, TEXT_PLAIN, base64Encoded(APPLICATION_OCTET_STREAM));

      assertSame(utf8, toOctetStreamFromByteArray);
      assertEquals(Base16Codec.encode(utf8), toOctetStreamHexFromByteArray);
      assertSame(Base64.getEncoder().encode(utf8), toOctetStream64FromByteArray);

      Object fromOctetStream = defaultTranscoder.transcode(toOctetStream, APPLICATION_OCTET_STREAM, TEXT_PLAIN);
      Object fromOctetStreamHex = defaultTranscoder.transcode(toOctetStreamHex, hexEncoded(APPLICATION_OCTET_STREAM), TEXT_PLAIN);
      Object fromOctetStream64 = defaultTranscoder.transcode(toOctetStream64, base64Encoded(APPLICATION_OCTET_STREAM), TEXT_PLAIN);

      assertSame(utf8, fromOctetStream);
      assertSame(utf8, fromOctetStreamHex);
      assertSame(utf8, fromOctetStream64);

   }

   @Test
   public void testTextUrlEncoded() throws Exception {
      String textData = "this is text";
      byte[] urlEncoded = URLEncoder.encode(textData, "utf-8").getBytes(UTF_8);

      Object encodedText = defaultTranscoder.transcode(textData, TEXT_PLAIN, APPLICATION_WWW_FORM_URLENCODED);
      assertSame(urlEncoded, encodedText);

      Object toText = defaultTranscoder.transcode(encodedText, APPLICATION_WWW_FORM_URLENCODED, TEXT_PLAIN);
      assertSame(textData.getBytes(UTF_8), toText);
   }


   private MediaType hexEncoded(MediaType mediaType) {
      return mediaType.withParameter("encoding", "hex");
   }

   private MediaType base64Encoded(MediaType mediaType) {
      return mediaType.withParameter("encoding", "base64");
   }

   private void assertSame(Object expected, Object obtained) {
      if (expected instanceof byte[]) {
         assertArrayEquals((byte[]) expected, (byte[]) obtained);
      } else {
         assertEquals(expected, obtained);
      }
   }

}
