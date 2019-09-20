package org.infinispan.commons.dataconversion;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_16;
import static java.nio.charset.StandardCharsets.UTF_16BE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.dataconversion.StandardConversions.bytesToHex;
import static org.infinispan.commons.dataconversion.StandardConversions.hexToBytes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.Instant;
import java.util.Calendar;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class StandardConversionsTest {

   @Rule
   public ExpectedException thrown = ExpectedException.none();

   @Test
   public void textToTextConversion() {
      String source = "All those moments will be lost in time, like tears in rain.";
      byte[] sourceAs8859 = source.getBytes(ISO_8859_1);
      byte[] sourceAsASCII = source.getBytes(US_ASCII);

      Object result = StandardConversions.convertTextToText(sourceAs8859,
            TEXT_PLAIN.withCharset(ISO_8859_1),
            TEXT_PLAIN.withCharset(US_ASCII));

      assertArrayEquals(sourceAsASCII, (byte[]) result);
   }

   @Test
   public void testTextToOctetStreamConversion() {
      String source = "Like our owl?";

      byte[] result = StandardConversions.convertTextToOctetStream(source, TEXT_PLAIN);

      assertArrayEquals(source.getBytes(UTF_8), result);
   }

   @Test
   public void testTextToObjectConversion() {
      String source = "Can the maker repair what he makes?";
      String source2 = "I had your job once. I was good at it.";

      byte[] sourceBytes = source2.getBytes(US_ASCII);

      Object result = StandardConversions.convertTextToObject(source, APPLICATION_OBJECT);
      Object result2 = StandardConversions.convertTextToObject(sourceBytes, TEXT_PLAIN.withCharset(US_ASCII));

      assertEquals(source, result);
      assertEquals(source2, result2);
   }

   @Test
   public void testTextToURLEncodedConversion() throws UnsupportedEncodingException {
      String source = "They're either a benefit or a hazard. If they're a benefit, it's not my problem.";
      String result = StandardConversions.convertTextToUrlEncoded(source, TEXT_PLAIN.withCharset(UTF_16));

      assertEquals(URLEncoder.encode(source, "UTF-16"), result);
   }

   @Test
   public void testOctetStreamToTextConversion() {
      String text = "Have you ever retired a human by mistake?";
      byte[] bytes1 = text.getBytes();
      byte[] bytes2 = new byte[]{1, 2, 3};

      byte[] result1 = StandardConversions.convertOctetStreamToText(bytes1, TEXT_PLAIN.withCharset(US_ASCII));
      byte[] result2 = StandardConversions.convertOctetStreamToText(bytes2, TEXT_PLAIN);

      assertArrayEquals(text.getBytes(US_ASCII), result1);
      assertArrayEquals(new String(bytes2).getBytes(UTF_8), result2);
   }

   @Test
   public void testOctetStreamToJavaConversion() {
      String value = "It's not an easy thing to meet your maker.";
      byte[] textStream = value.getBytes(UTF_8);
      byte[] randomBytes = new byte[]{23, 23, 34, 1, -1, -123};

      Marshaller marshaller = new ProtoStreamMarshaller();

      MediaType stringType = APPLICATION_OBJECT.withParameter("type", "java.lang.String");
      Object result = StandardConversions.convertOctetStreamToJava(textStream, stringType, marshaller);
      assertEquals(value, result);

      MediaType byteArrayType = APPLICATION_OBJECT.withParameter("type", "ByteArray");
      Object result2 = StandardConversions.convertOctetStreamToJava(textStream, byteArrayType, marshaller);
      assertArrayEquals(textStream, (byte[]) result2);

      Object result3 = StandardConversions.convertOctetStreamToJava(randomBytes, byteArrayType, marshaller);
      assertArrayEquals(randomBytes, (byte[]) result3);

      thrown.expect(EncodingException.class);
      MediaType doubleType = APPLICATION_OBJECT.withParameter("type", "java.lang.Double");
      StandardConversions.convertOctetStreamToJava(randomBytes, doubleType, marshaller);
      System.out.println(thrown);
   }

   @Test
   public void testJavaToTextConversion() {
      String string = "I've seen things you people wouldn't believe.";
      Double number = 12.1d;
      Calendar complex = Calendar.getInstance();

      MediaType stringType = APPLICATION_OBJECT.withParameter("type", "java.lang.String");
      byte[] result1 = StandardConversions.convertJavaToText(string, stringType, TEXT_PLAIN.withCharset(UTF_16BE));
      assertArrayEquals(string.getBytes(UTF_16BE), result1);

      MediaType doubleType = APPLICATION_OBJECT.withParameter("type", "java.lang.Double");
      byte[] result2 = StandardConversions.convertJavaToText(number, doubleType, TEXT_PLAIN.withCharset(US_ASCII));
      assertArrayEquals("12.1".getBytes(US_ASCII), result2);

      MediaType customType = APPLICATION_OBJECT.withParameter("type", complex.getClass().getName());
      byte[] bytes = StandardConversions.convertJavaToText(complex, customType, TEXT_PLAIN.withCharset(US_ASCII));
      assertEquals(complex.toString(), new String(bytes));
   }


   @Test
   public void testJavaToOctetStreamConversion() throws IOException, InterruptedException {
      Marshaller marshaller = new ProtoStreamMarshaller();
      String string = "I've seen things you people wouldn't believe.";
      Double number = 12.1d;
      Instant complex = Instant.now();
      byte[] binary = new byte[]{1, 2, 3};

      MediaType stringType = APPLICATION_OBJECT.withParameter("type", "java.lang.String");
      byte[] result = StandardConversions.convertJavaToOctetStream(string, stringType, marshaller);
      assertArrayEquals(string.getBytes(UTF_8), result);

      MediaType doubleType = APPLICATION_OBJECT.withParameter("type", "java.lang.Double");
      result = StandardConversions.convertJavaToOctetStream(number, doubleType, marshaller);
      assertArrayEquals(marshaller.objectToByteBuffer(number), result);

      MediaType customType = APPLICATION_OBJECT.withParameter("type", complex.getClass().getName());
      result = StandardConversions.convertJavaToOctetStream(complex, customType, marshaller);
      assertArrayEquals(marshaller.objectToByteBuffer(complex), result);

      MediaType byteArrayType = APPLICATION_OBJECT.withParameter("type", "ByteArray");
      result = StandardConversions.convertJavaToOctetStream(binary, byteArrayType, marshaller);
      assertArrayEquals(result, binary);
   }

   @Test
   public void testHexConversion() throws Exception {
      assertNull(bytesToHex(null));
      assertNull(hexToBytes(null));
      assertEquals("", bytesToHex(new byte[]{}));
      assertArrayEquals(new byte[]{}, hexToBytes(""));

      Marshaller marshaller = new ProtoStreamMarshaller();
      byte[] before = marshaller.objectToByteBuffer(Instant.now());
      byte[] after = hexToBytes(bytesToHex(before));
      assertArrayEquals(before, after);

      assertEquals("0x010203", bytesToHex(new byte[]{1, 2, 3}));
      assertEquals("0x808080", bytesToHex(new byte[]{-128, -128, -128}));
   }
}
