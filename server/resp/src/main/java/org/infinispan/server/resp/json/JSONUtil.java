package org.infinispan.server.resp.json;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;

public class JSONUtil {
   private static byte JSON_ROOT_BYTE = '$';
   public static byte[] JSON_ROOT = new byte[] { '$' };

   public static final ObjectMapper objectMapper = JsonMapper.builder()
         .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
         .build();

   // Shared provider instances - all use the same configured objectMapper
   private static final com.jayway.jsonpath.spi.mapper.JacksonMappingProvider mappingProvider =
         new com.jayway.jsonpath.spi.mapper.JacksonMappingProvider(objectMapper);
   private static final com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider jsonNodeProvider =
         new com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider(objectMapper);
   private static final InfinispanJacksonJsonNodeProvider infinispanJsonNodeProvider =
         new InfinispanJacksonJsonNodeProvider(objectMapper);

   // JSONPath config that returns null instead of failing for missing leaf. Used
   // to set new leaf for undefined
   // path.
   // SET operation requires two different config:
   // - definite path needs DEFAULT_PATH_LEAF_TO_NULL otherwise can't add leaf node
   // - undefinite path doesn't need, because it doesn't add node if missing, it
   // just
   // update existing node
   public static final Configuration configForDefiniteSet = Configuration.builder().options(Option.SUPPRESS_EXCEPTIONS)
         .options(Option.DEFAULT_PATH_LEAF_TO_NULL)
         .jsonProvider(infinispanJsonNodeProvider)
         .mappingProvider(mappingProvider).build();
   public static ParseContext parserForDefiniteSet = JsonPath.using(configForDefiniteSet);

   public static final Configuration configForSet = Configuration.builder().options(Option.SUPPRESS_EXCEPTIONS)
         .jsonProvider(jsonNodeProvider)
         .mappingProvider(mappingProvider).build();
   public static ParseContext parserForSet = JsonPath.using(configForSet);

   // GET operation needs ALWAYS_RETURN_LIST to handle multipath results
   public static final Configuration configForGet = Configuration.builder().options(Option.ALWAYS_RETURN_LIST)
         .options(Option.SUPPRESS_EXCEPTIONS).jsonProvider(infinispanJsonNodeProvider)
         .mappingProvider(mappingProvider).build();
   public static ParseContext parserForGet = JsonPath.using(configForGet);

   // Modifier operations need ALWAYS_RETURN_LIST and AS_PATH_LIST
   public static final Configuration configForMod = Configuration.builder().options(Option.AS_PATH_LIST)
         .options(Option.SUPPRESS_EXCEPTIONS)
         .jsonProvider(jsonNodeProvider)
         .mappingProvider(mappingProvider).build();
   public static ParseContext parserForMod = JsonPath.using(configForMod);

   // Modifier operations need ALWAYS_RETURN_LIST and AS_PATH_LIST
   public static final Configuration configForDefiniteMod = Configuration.builder().options(Option.AS_PATH_LIST)
         .options(Option.SUPPRESS_EXCEPTIONS).options(Option.DEFAULT_PATH_LEAF_TO_NULL)
         .jsonProvider(jsonNodeProvider)
         .mappingProvider(mappingProvider).build();
   public static ParseContext parserForDefiniteMod = JsonPath.using(configForMod);

   public static boolean isRoot(byte[] path) {
      return path != null && path.length == 1 && path[0] == JSON_ROOT_BYTE;
   }

   /**
    * Returns a jsonpath version of path with Redis regex patterns converted to
    * Jayway format.
    *
    * This method performs two conversions in order: 1. Legacy path format
    * conversion (e.g., ".foo"
    * → "$.foo") 2. Redis regex format conversion on the result (e.g., =~ "pattern"
    * → =~ /pattern/)
    *
    * @param path
    *             the path as a String
    * @return path converted to Jayway-compatible JSONPath format
    */
   public static String toJsonPath(String path) {
      // Step 1: Handle legacy path format conversion (. → $)
      String step1Result;
      if (!isJsonPath(path)) {
         if (path.startsWith(".")) {
            if (path.length() == 1) {
               // For '.' return json root '$'
               step1Result = "$";
            } else {
               // Just prepend $ to the beginning
               step1Result = "$" + path;
            }
         } else {
            // prepend $. to the beginning
            // Using "$." so wrong legacy path like "" and " " will fail
            step1Result = "$." + path;
         }
      } else {
         step1Result = path;
      }

      // Step 2: Convert Redis regex format to Jayway format
      return convertRedisFilterToJayway(step1Result);
   }

   /**
    * Returns a jsonpath version of path with Redis regex patterns converted to
    * Jayway format.
    *
    * This is a convenience method that converts byte[] to String, performs the
    * conversions, and
    * returns the result as byte[].
    *
    * @param path
    *             the path as byte[]
    * @return path converted to Jayway-compatible JSONPath format as byte[]
    */
   public static byte[] toJsonPath(byte[] path) {
      // Special case optimization: single '.' becomes '$' without String conversion
      if (path != null && path.length == 1 && path[0] == '.') {
         return JSON_ROOT;
      }

      // Convert to String, apply conversions, convert back
      String pathStr = new String(path, StandardCharsets.UTF_8);
      String converted = toJsonPath(pathStr);
      return converted.equals(pathStr) ? path : converted.getBytes(StandardCharsets.UTF_8);
   }

   public static boolean isJsonPath(byte[] path) {
      return (path != null && path.length > 0 && path[0] == JSON_ROOT_BYTE
            && (path.length < 2 || path[1] == '.' || path[1] == '['));
   }

   public static boolean isJsonPath(String path) {
      return path == null ? false : isJsonPath(path.getBytes(StandardCharsets.UTF_8));
   }

   // Invalid values for Redis. Expecially '\0xa' breaks RESP, seen as end of data
   public static boolean isValueInvalid(byte[] value) {
      if (value.length == 0)
         return true;
      if (value.length == 1) {
         return isSingleCharInvalid(value[0]);
      }
      if (value.length == 2) {
         return isDoubleCharInvalid(value);
      }
      return false;
   }

   private static boolean isSingleCharInvalid(byte value) {
      switch (value) {
         case ' ':
         case '{':
         case '}':
         case '[':
         case ']':
         case '\\':
         case '\'':
         case 0:
         case 0x0a:
         case 0x0c:
            return true;
         default:
            return false;
      }
   }

   private static boolean isDoubleCharInvalid(byte[] value) {
      if (value[0] == '\\' && (value[1] == '\\' || value[1] == '"' || value[1] == '[')) {
         return true;
      }
      if (value[0] == '{' && value[1] == ']') {
         return true;
      }
      if (value[0] == '[' && value[1] == '}') {
         return true;
      }
      return false;
   }

   // --- Base regex converter (from before) ---
   private static String toJaywayRegex(String redisRegex) {
      if (redisRegex == null || redisRegex.isBlank()) {
         throw new IllegalArgumentException("Regex cannot be null or empty");
      }

      // Trim quotes if present
      String pattern = redisRegex.trim();
      boolean isQuoted = pattern.startsWith("\"") && pattern.endsWith("\"");
      if (isQuoted && pattern.length() >= 2) {
         pattern = pattern.substring(1, pattern.length() - 1);
      }

      boolean startsWithAnchor = pattern.startsWith("^");
      boolean endsWithAnchor = pattern.endsWith("$");
      boolean hasLeadingWildcard = pattern.startsWith(".*");
      boolean hasTrailingWildcard = pattern.endsWith(".*");

      if (isQuoted) {
         // Add missing wildcards only if needed
         if (!startsWithAnchor && !hasLeadingWildcard) {
            pattern = ".*" + pattern;
         }
         if (!endsWithAnchor && !hasTrailingWildcard) {
            pattern = pattern + ".*";
         }
         // Wrap in delimiters
         return "/" + pattern + "/";
      }
      return pattern;
   }

   // --- Filter-level converter ---
   /**
    * Converts any RedisJSON-style =~ "regex" fragments into Jayway =~ /regex/
    * format. Keeps the
    * rest of the JSONPath filter untouched.
    */

   public static String convertRedisFilterToJayway(String redisFilter) {
      if (redisFilter == null || redisFilter.isBlank()) {
         throw new IllegalArgumentException("Filter cannot be null or empty");
      }

      // Match patterns like =~ "something"
      Pattern p = Pattern.compile("=~\\s*\"([^\"]+)\"");
      Matcher m = p.matcher(redisFilter);

      StringBuilder sb = new StringBuilder();
      while (m.find()) {
         String redisRegex = "\"" + m.group(1) + "\"";
         String jaywayRegex = toJaywayRegex(redisRegex);
         m.appendReplacement(sb, "=~ " + jaywayRegex);
      }
      m.appendTail(sb);

      return sb.toString();
   }

}
