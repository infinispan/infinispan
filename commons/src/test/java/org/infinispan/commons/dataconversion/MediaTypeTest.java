package org.infinispan.commons.dataconversion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

/**
 * @since 9.2
 */
public class MediaTypeTest {

   @Test
   public void testParsingTypeSubType() throws Exception {
      MediaType appJson = MediaType.fromString("application/json");

      assertMediaTypeNoParams(appJson, "application", "json");
   }

   @Test(expected = EncodingException.class)
   public void testParsingEmpty() throws Exception {
      MediaType.fromString("");
   }

   @Test(expected = EncodingException.class)
   public void testParsingNoSubType() throws Exception {
      MediaType.fromString("something");
   }

   @Test(expected = EncodingException.class)
   public void testParsingNoSubType2() throws Exception {
      MediaType.fromString("application; charset=utf-8");
   }

   @Test(expected = EncodingException.class)
   public void testParsingNull() throws Exception {
      MediaType.fromString(null);
   }

   @Test
   public void testParsingEmptySpaces() throws Exception {
      MediaType appJson = MediaType.fromString("application /json");
      assertMediaTypeNoParams(appJson, "application", "json");
   }

   @Test
   public void testParsingEmptySpaces2() throws Exception {
      MediaType appJson = MediaType.fromString("application/ json");
      assertMediaTypeNoParams(appJson, "application", "json");
   }

   @Test
   public void testParsingEmptySpaces3() throws Exception {
      MediaType appJson = MediaType.fromString("application  / json");
      assertMediaTypeNoParams(appJson, "application", "json");
   }

   @Test
   public void testQuotedParams() throws Exception {
      MediaType mediaType = MediaType.fromString("application/json; charset=\"UTF-8\"");

      assertMediaTypeWithParam(mediaType, "application", "json", "charset", "UTF-8");
   }

   @Test
   public void testQuotedParams2() throws Exception {
      MediaType mediaType = MediaType.fromString("application/json; charset='UTF-8'");

      assertMediaTypeWithParam(mediaType, "application", "json", "charset", "UTF-8");
   }

   @Test
   public void testUnQuotedParam() throws Exception {
      MediaType mediaType = MediaType.fromString("application/json; charset=UTF-8");

      assertMediaTypeWithParam(mediaType, "application", "json", "charset", "UTF-8");
   }

   @Test
   public void testToString() throws Exception {
      assertEquals("text/csv", new MediaType("text", "csv").toString());
      assertEquals("foo/bar; a=2", new MediaType("foo", "bar", createMap(new MapEntry("a", "2"))).toString());
      assertEquals("foo/bar; a=2; b=1; c=2", new MediaType("foo", "bar",
            createMap(new MapEntry("a", "2"), new MapEntry("b", "1"), new MapEntry("c", "2"))).toString());

   }

   @Test
   public void testUnQuotedParamWithSpaces() throws Exception {
      MediaType mediaType = MediaType.fromString("application/json ; charset= UTF-8");

      assertMediaTypeWithParam(mediaType, "application", "json", "charset", "UTF-8");
   }

   @Test(expected = EncodingException.class)
   public void testWrongQuoting() throws Exception {
      MediaType.fromString("application/json ; charset= \"UTF-8");
   }

   @Test
   public void testMultipleParameters() throws Exception {
      MediaType mediaType = MediaType.fromString("application/json ; charset=UTF-8; param1=value1; param2 = value2");
      assertMediaTypeWithParams(mediaType, "application", "json",
            new String[]{"charset", "param1", "param2"},
            new String[]{"UTF-8", "value1", "value2"});
   }

   @Test(expected = EncodingException.class)
   public void testMultipleParametersWrongSeparator() throws Exception {
      MediaType.fromString("application/json ; charset=UTF-8; param1=value1, param2 = value2");
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

}
