package org.infinispan.commons.dataconversion;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.test.Exceptions;
import org.junit.Test;

/**
 * @since 9.2
 */
public class MediaTypeTest {

   @Test
   public void testParsingTypeSubType() {
      MediaType appJson = MediaType.fromString("application/json");

      assertMediaTypeNoParams(appJson, "application", "json");
   }

   @Test(expected = EncodingException.class)
   public void testParsingMultipleSubType() {
      MediaType.fromString("application/json/on");
   }

   @Test
   public void testParsingNoType() {
      Exceptions.expectException(EncodingException.class, () -> MediaType.fromString(""));

      Exceptions.expectException(EncodingException.class, () -> MediaType.fromString(";param=value"));
   }

   @Test(expected = EncodingException.class)
   public void testParsingNoSubType() {
      MediaType.fromString("something");
   }

   @Test(expected = EncodingException.class)
   public void testParsingNoSubType2() {
      MediaType.fromString("application; charset=utf-8");
   }

   @Test(expected = EncodingException.class)
   public void testParsingNull() {
      MediaType.fromString(null);
   }

   @Test(expected = EncodingException.class)
   public void testParsingWhitespaceInType() {
      MediaType.fromString("application /json");
   }

   @Test(expected = EncodingException.class)
   public void testParsingWhitespaceInSubtype() {
      MediaType.fromString("application/ json");
   }

   @Test(expected = EncodingException.class)
   public void testParsingWhitespaceInTypeSubtype() {
      MediaType.fromString("application  / json");
   }

   @Test(expected = EncodingException.class)
   public void testParsingWhitespaceInParamName() {
      MediaType.fromString("application/json; charset =utf-8");
   }

   @Test
   public void testQuotedParam() {
      MediaType mediaType = MediaType.fromString("application/json; charset=\"UTF-8\"");

      assertMediaTypeWithParam(mediaType, "application", "json", "charset", "\"UTF-8\"");
   }

   @Test
   public void testQuotedParamsWithWhitespace() {
      MediaType mediaType = MediaType.fromString("application/json; charset=\" UTF-8 \"");

      assertMediaTypeWithParam(mediaType, "application", "json", "charset", "\" UTF-8 \"");
   }

   @Test
   public void testQuotedParamsEscaping() {
      MediaType mediaType = MediaType.fromString("application/json; charset=\"\\\"UTF-8\\\"\"");
      assertMediaTypeWithParam(mediaType, "application", "json", "charset", "\"\\\"UTF-8\\\"\"");

      MediaType mediaType2 = MediaType.fromString("application/json; charset=\"\\a\\\"\\\\\"");
      assertMediaTypeWithParam(mediaType2, "application", "json", "charset", "\"\\a\\\"\\\\\"");

      Exceptions.expectException(EncodingException.class, () -> MediaType.fromString("application/json; charset=\"\\\""));
   }

   @Test
   public void testUnquotedParamWithSingleQuote() {
      MediaType mediaType = MediaType.fromString("application/json; charset='UTF-8'");
      assertMediaTypeWithParam(mediaType, "application", "json", "charset", "'UTF-8'");

      Exceptions.expectException(EncodingException.class, () -> MediaType.fromString("application/json; charset='UTF 8'"));
   }

   @Test
   public void testUnQuotedParam() {
      MediaType mediaType = MediaType.fromString("application/json; charset=UTF-8");
      assertMediaTypeWithParam(mediaType, "application", "json", "charset", "UTF-8");

      Exceptions.expectException(EncodingException.class, () -> MediaType.fromString("application/json; charset=utf 8"));
   }

   @Test
   public void testToString() {
      assertEquals("application/xml; q=0.9",
            new MediaType("application", "xml", Map.of("q", "0.9")).toString());
      assertEquals("text/csv", new MediaType("text", "csv").toString());
      assertEquals("foo/bar; a=2", new MediaType("foo", "bar", Map.of("a", "2")).toString());
      String type = new MediaType("foo", "bar", Map.of("a", "2", "b", "1", "c", "2")).toString();
      assertTrue(type.startsWith("foo/bar; "));
      assertTrue(type.contains("; a=2"));
      assertTrue(type.contains("; b=1"));
      assertTrue(type.contains("; c=2"));
      assertEquals("a/b; p=1", MediaType.fromString("a/b; p=1; q=2").toStringExcludingParam("q"));
   }

   @Test
   public void testToStringQuotedParams() {
      MediaType mediaType = MediaType.fromString("a/b; p=\"\"; r=\"\\\"\"");
      assertEquals(mediaType, MediaType.fromString(mediaType.toString()));
   }

   @Test(expected = EncodingException.class)
   public void testUnQuotedParamWithSpaces() {
      MediaType mediaType = MediaType.fromString("application/json ; charset= UTF-8");
   }

   @Test(expected = EncodingException.class)
   public void testWrongQuoting() {
      MediaType.fromString("application/json;charset= \"UTF-8");
   }

   @Test
   public void testMultipleParameters() {
      MediaType mediaType = MediaType.fromString("application/json; charset=UTF-8; param1=value1 ;param2=value2");
      assertMediaTypeWithParams(mediaType, "application", "json",
            new String[]{"charset", "param1", "param2"},
            new String[]{"UTF-8", "value1", "value2"});
   }

   @Test(expected = EncodingException.class)
   public void testMultipleParametersWrongSeparator() {
      MediaType.fromString("application/json; charset=UTF-8; param1=value1, param2=value2");
   }

   @Test
   public void testParseWeight() {
      MediaType mediaType = MediaType.fromString("application/json; q=0.8");
      assertEquals(0.8, mediaType.getWeight(), 0.0);
   }

   @Test(expected = EncodingException.class)
   public void testParseInvalidWeight() {
      MediaType.fromString("application/json ; q=high");
   }

   @Test
   public void testDefaultWeight() {
      MediaType mediaType = MediaType.fromString("application/json");
      assertEquals(1.0, mediaType.getWeight(), 0.0);
   }

   @Test
   public void testWildCard() {
      MediaType mediaType = MediaType.fromString("*/*");

      assertEquals("*", mediaType.getType());
      assertEquals("*", mediaType.getSubType());
      assertTrue(mediaType.match(MediaType.TEXT_PLAIN));
      assertTrue(mediaType.match(MediaType.APPLICATION_PROTOSTREAM));
   }

   @Test
   public void testWildCard2() {
      MediaType mediaType = MediaType.fromString("*");

      assertEquals("*", mediaType.getType());
      assertEquals("*", mediaType.getSubType());
   }

   @Test
   public void testParseList() {
      List<MediaType> mediaTypes = MediaType.parseList("text/html, image/png,*/*").collect(Collectors.toList());
      assertEquals(asList(MediaType.TEXT_HTML, MediaType.IMAGE_PNG, MediaType.MATCH_ALL), mediaTypes);

      Exceptions.expectException(EncodingException.class, () -> MediaType.parseList("text/html,"));
      Exceptions.expectException(EncodingException.class, () -> MediaType.parseList("text/html;a=b,"));
      Exceptions.expectException(EncodingException.class, () -> MediaType.parseList("text/html;a=b,;b=c"));
      Exceptions.expectException(EncodingException.class, () -> MediaType.parseList("text/html;a=b,image/png/"));

      List<MediaType> mediaTypes2 = MediaType.parseList("text/html;a=\"b,\" ,*/*").collect(Collectors.toList());
      assertEquals(asList(MediaType.TEXT_HTML.withParameter("a", "\"b,\""), MediaType.MATCH_ALL), mediaTypes2);
   }

   @Test
   public void testParseBrowserRequest() {
      Stream<MediaType> list = MediaType.parseList("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
      Iterator<MediaType> iterator = list.iterator();

      assertEquals("text/html", iterator.next().getTypeSubtype());
      assertEquals("application/xhtml+xml", iterator.next().getTypeSubtype());
      assertEquals("application/xml", iterator.next().getTypeSubtype());
      assertEquals("*/*", iterator.next().getTypeSubtype());
   }

   @Test
   public void testParseSingleAsterix() {
      List<String> list = MediaType.parseList("text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2")
                                      .map(MediaType::getTypeSubtype)
                                      .collect(Collectors.toList());
      assertEquals(asList("text/html", "image/gif", "image/jpeg", "*/*", "*/*"), list);
   }

   @Test
   public void testNegotiations() {
      Stream<MediaType> mediaTypes = MediaType.parseList("text/html; q=0.8,*/*;q=0.2,application/json");

      Iterator<MediaType> iterator = mediaTypes.iterator();

      MediaType preferred = iterator.next();
      MediaType secondChoice = iterator.next();
      MediaType everythingElse = iterator.next();

      assertEquals(MediaType.APPLICATION_JSON, preferred);
      assertEquals("text/html", secondChoice.getTypeSubtype());
      assertEquals("*/*", everythingElse.getTypeSubtype());
   }

   @Test
   public void testMediaTypeMatch() {
      MediaType one = MediaType.APPLICATION_JSON;
      MediaType two = MediaType.APPLICATION_JSON;

      assertTrue(one.match(two));
      assertTrue(two.match(one));
   }

   @Test
   public void testMediaTypeUnMatch() {
      MediaType one = MediaType.APPLICATION_JSON;
      MediaType two = MediaType.APPLICATION_JAVASCRIPT;

      assertFalse(one.match(two));
      assertFalse(two.match(one));
   }

   @Test
   public void testMediaTypeMatchItself() {
      MediaType one = MediaType.APPLICATION_JSON;
      assertTrue(one.match(one));
   }

   private void assertMediaTypeNoParams(MediaType mediaType, String type, String subType) {
      assertEquals(type, mediaType.getType());
      assertEquals(subType, mediaType.getSubType());
      assertFalse(mediaType.hasParameters());
      assertEquals(Optional.empty(), mediaType.getParameter("a"));
   }

   private void assertMediaTypeWithParam(MediaType mediaType, String type, String subType, String paramName, String paramValue) {
      assertMediaTypeWithParams(mediaType, type, subType, new String[]{paramName}, new String[]{paramValue});
   }

   private void assertMediaTypeWithParams(MediaType mediaType, String type, String subType, String[] paramNames, String[] paramValues) {
      assertEquals(type, mediaType.getType());
      assertEquals(subType, mediaType.getSubType());
      assertTrue(mediaType.hasParameters());
      for (int i = 0; i < paramNames.length; i++) {
         String paramName = paramNames[i];
         String paramValue = paramValues[i];
         assertEquals(Optional.of(paramValue), mediaType.getParameter(paramName));
      }
   }

   private static class ObjectInOut implements ObjectInput, ObjectOutput {

      private final Queue<Object> buffer = new LinkedList<>();

      @Override
      public Object readObject() throws ClassNotFoundException, IOException {
         return buffer.remove();
      }

      @Override
      public int read() {
         throw new UnsupportedOperationException();
      }

      @Override
      public int read(byte[] b) {
         throw new UnsupportedOperationException();
      }

      @Override
      public int read(byte[] b, int off, int len) {
         throw new UnsupportedOperationException();
      }

      @Override
      public long skip(long n) {
         throw new UnsupportedOperationException();
      }

      @Override
      public int available() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeObject(Object obj) throws IOException {
         buffer.add(obj);
      }

      @Override
      public void write(int b) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void write(byte[] b) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void write(byte[] b, int off, int len) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeBoolean(boolean v) {
         buffer.add(v);
      }

      @Override
      public void writeByte(int v) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeShort(int v) {
         buffer.add((short) v);
      }

      @Override
      public void writeChar(int v) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeInt(int v) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeLong(long v) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeFloat(float v) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeDouble(double v) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeBytes(String s) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeChars(String s) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeUTF(String s) {
         buffer.add(s);
      }

      @Override
      public void flush() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void close() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void readFully(byte[] b) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void readFully(byte[] b, int off, int len) {
         throw new UnsupportedOperationException();
      }

      @Override
      public int skipBytes(int n) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean readBoolean() {
         return (boolean) buffer.remove();
      }

      @Override
      public byte readByte() {
         throw new UnsupportedOperationException();
      }

      @Override
      public int readUnsignedByte() {
         throw new UnsupportedOperationException();
      }

      @Override
      public short readShort() {
         return (short) buffer.remove();
      }

      @Override
      public int readUnsignedShort() {
         throw new UnsupportedOperationException();
      }

      @Override
      public char readChar() {
         throw new UnsupportedOperationException();
      }

      @Override
      public int readInt() {
         throw new UnsupportedOperationException();
      }

      @Override
      public long readLong() {
         throw new UnsupportedOperationException();
      }

      @Override
      public float readFloat() {
         throw new UnsupportedOperationException();
      }

      @Override
      public double readDouble() {
         throw new UnsupportedOperationException();
      }

      @Override
      public String readLine() {
         throw new UnsupportedOperationException();
      }

      @Override
      public String readUTF() {
         return (String) buffer.remove();
      }
   }
}
