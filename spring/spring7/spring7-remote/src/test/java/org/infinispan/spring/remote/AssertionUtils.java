package org.infinispan.spring.remote;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Properties;

public class AssertionUtils {
   public static void assertPropertiesSubset(String message, Properties expected, Properties actual) {
      for (String key : expected.stringPropertyNames()) {
         assertTrue(String.format("%s Key %s missing from %s", message, key, actual), actual.containsKey(key));
         assertEquals(String.format("%s Key %s's expected value was \"%s\" but actual was \"%s\"", message, key, expected.getProperty(key), actual.getProperty(key)), expected.getProperty(key), actual.getProperty(key));
      }
   }
}
