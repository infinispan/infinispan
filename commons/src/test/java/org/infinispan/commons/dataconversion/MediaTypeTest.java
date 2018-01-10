package org.infinispan.commons.dataconversion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.Queue;

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
   public void testParsingEmpty() {
      MediaType.fromString("");
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

   @Test
   public void testParsingEmptySpaces() {
      MediaType appJson = MediaType.fromString("application /json");
      assertMediaTypeNoParams(appJson, "application", "json");
   }

   @Test
   public void testParsingEmptySpaces2() {
      MediaType appJson = MediaType.fromString("application/ json");
      assertMediaTypeNoParams(appJson, "application", "json");
   }

   @Test
   public void testParsingEmptySpaces3() {
      MediaType appJson = MediaType.fromString("application  / json");
      assertMediaTypeNoParams(appJson, "application", "json");
   }

   @Test
   public void testQuotedParams() {
      MediaType mediaType = MediaType.fromString("application/json; charset=\"UTF-8\"");

      assertMediaTypeWithParam(mediaType, "application", "json", "charset", "UTF-8");
   }

   @Test
   public void testQuotedParams2() {
      MediaType mediaType = MediaType.fromString("application/json; charset='UTF-8'");

      assertMediaTypeWithParam(mediaType, "application", "json", "charset", "UTF-8");
   }

   @Test
   public void testUnQuotedParam() {
      MediaType mediaType = MediaType.fromString("application/json; charset=UTF-8");

      assertMediaTypeWithParam(mediaType, "application", "json", "charset", "UTF-8");
   }

   @Test
   public void testToString() {
      assertEquals("application/xml", new MediaType("application", "xml", createMap(new MapEntry("q", "0.9"))).toString());
      assertEquals("text/csv", new MediaType("text", "csv").toString());
      assertEquals("foo/bar; a=2", new MediaType("foo", "bar", createMap(new MapEntry("a", "2"))).toString());
      assertEquals("foo/bar; a=2; b=1; c=2", new MediaType("foo", "bar",
            createMap(new MapEntry("a", "2"), new MapEntry("b", "1"), new MapEntry("c", "2"))).toString());
      assertEquals("a/b; p=1", MediaType.fromString("a/b; p=1; q=2;").toStringExcludingParam("q"));
   }

   @Test
   public void testUnQuotedParamWithSpaces() {
      MediaType mediaType = MediaType.fromString("application/json ; charset= UTF-8");

      assertMediaTypeWithParam(mediaType, "application", "json", "charset", "UTF-8");
   }

   @Test(expected = EncodingException.class)
   public void testWrongQuoting() {
      MediaType.fromString("application/json ; charset= \"UTF-8");
   }

   @Test
   public void testMultipleParameters() {
      MediaType mediaType = MediaType.fromString("application/json ; charset=UTF-8; param1=value1; param2 = value2");
      assertMediaTypeWithParams(mediaType, "application", "json",
            new String[]{"charset", "param1", "param2"},
            new String[]{"UTF-8", "value1", "value2"});
   }

   @Test(expected = EncodingException.class)
   public void testMultipleParametersWrongSeparator() {
      MediaType.fromString("application/json ; charset=UTF-8; param1=value1, param2 = value2");
   }

   @Test
   public void testParseWeight() {
      MediaType mediaType = MediaType.fromString("application/json ; q=0.8");
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
   public void testParseList() {
      Stream<MediaType> mediaTypes = MediaType.parseList("text/html, image/png,*/*");
      Iterator<MediaType> mediaTypeIterator = mediaTypes.iterator();

      assertEquals(MediaType.TEXT_HTML, mediaTypeIterator.next());
      assertEquals(MediaType.IMAGE_PNG, mediaTypeIterator.next());
      assertEquals(MediaType.MATCH_ALL, mediaTypeIterator.next());
      assertFalse(mediaTypeIterator.hasNext());
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
      MediaType one = MediaType.APPLICATION_INFINISPAN_BINARY;
      MediaType two = MediaType.APPLICATION_INFINISPAN_BINARY;

      assertTrue(one.match(two));
      assertTrue(two.match(one));
   }

   @Test
   public void testMediaTypeUnMatch() {
      MediaType one = MediaType.APPLICATION_INFINISPAN_BINARY;
      MediaType two = MediaType.APPLICATION_INFINISPAN_MARSHALLED;

      assertFalse(one.match(two));
      assertFalse(two.match(one));
   }

   @Test
   public void testMediaTypeMatchItself() {
      MediaType one = MediaType.APPLICATION_INFINISPAN_BINARY;
      assertTrue(one.match(one));
   }

   @Test
   public void testMediaTypeExternalizerNoId() throws Exception {
      ObjectInOut inOutOrig = new ObjectInOut();
      MediaType.MediaTypeExternalizer mediaTypeExternalizer = new MediaType.MediaTypeExternalizer();
      mediaTypeExternalizer.writeObject(inOutOrig, MediaType.APPLICATION_XML);
      MediaType mediaType = mediaTypeExternalizer.readObject(inOutOrig);

      assertMediaTypeNoParams(mediaType, "application", "xml");
   }

   @Test
   public void testMediaTypeExternalizerId() throws Exception {
      ObjectInOut inOutOrig = new ObjectInOut();
      MediaType.MediaTypeExternalizer mediaTypeExternalizer = new MediaType.MediaTypeExternalizer();
      mediaTypeExternalizer.writeObject(inOutOrig, MediaType.TEXT_PLAIN);
      MediaType mediaType = mediaTypeExternalizer.readObject(inOutOrig);

      assertMediaTypeNoParams(mediaType, "text", "plain");
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

   private class MapEntry {
      private String key;
      private String value;

      String getKey() {
         return key;
      }

      String getValue() {
         return value;
      }

      MapEntry(String key, String value) {
         this.key = key;
         this.value = value;
      }
   }

   private static Map<String, String> createMap(MapEntry... entries) {
      Map<String, String> map = new HashMap<>();
      Arrays.stream(entries).forEach(e -> map.put(e.getKey(), e.getValue()));
      return map;
   }

   private static class ObjectInOut implements ObjectInput, ObjectOutput {

      private final Queue<Object> buffer = new LinkedList<>();

      @Override
      public Object readObject() throws ClassNotFoundException, IOException {
         return buffer.poll();
      }

      @Override
      public int read() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public int read(byte[] b) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public long skip(long n) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public int available() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeObject(Object obj) throws IOException {
         buffer.add(obj);
      }

      @Override
      public void write(int b) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void write(byte[] b) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeBoolean(boolean v) throws IOException {
         buffer.add(v);
      }

      @Override
      public void writeByte(int v) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeShort(int v) throws IOException {
         buffer.add((short) v);
      }

      @Override
      public void writeChar(int v) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeInt(int v) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeLong(long v) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeFloat(float v) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeDouble(double v) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeBytes(String s) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeChars(String s) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeUTF(String s) throws IOException {
         buffer.add(s);
      }

      @Override
      public void flush() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void close() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void readFully(byte[] b) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void readFully(byte[] b, int off, int len) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public int skipBytes(int n) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean readBoolean() throws IOException {
         return (boolean) buffer.poll();
      }

      @Override
      public byte readByte() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public int readUnsignedByte() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public short readShort() throws IOException {
         return (short) buffer.poll();
      }

      @Override
      public int readUnsignedShort() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public char readChar() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public int readInt() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public long readLong() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public float readFloat() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public double readDouble() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public String readLine() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public String readUTF() throws IOException {
         return (String) buffer.poll();
      }
   }
}
