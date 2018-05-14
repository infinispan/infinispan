package org.infinispan.commons.logging.log4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;

@Plugin(name = "testName", category = "Lookup")
public class TestNameLookup implements StrLookup {
   Map<String, String> cache = new ConcurrentHashMap<>();
   Pattern pattern = Pattern.compile("\\b(\\w+Test)\\b");

   public static final String TEST_NAME = "testName";

   public String lookup(String key) {
      return TEST_NAME;
   }

   public String lookup(LogEvent event, String key) {
      String testName = cache.computeIfAbsent(event.getThreadName(), threadName -> {
         Matcher matcher = pattern.matcher(event.getThreadName());
         return matcher.find() ? matcher.group(1) : null;
      });
      return testName;
   }
}
