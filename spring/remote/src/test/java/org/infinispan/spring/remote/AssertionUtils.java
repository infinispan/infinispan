package org.infinispan.spring.remote;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;

public class AssertionUtils {
   public static void assertPropertiesSubset(Properties expected, Properties actual, String message) {
      for (String key : expected.stringPropertyNames()) {
         assertTrue(actual.containsKey(key), String.format("%s Key %s missing from %s", message, key, actual));
         assertEquals(expected.getProperty(key), actual.getProperty(key), String.format("%s Key %s's expected value was \"%s\" but actual was \"%s\"", message, key, expected.getProperty(key), actual.getProperty(key)));
      }
   }
}
