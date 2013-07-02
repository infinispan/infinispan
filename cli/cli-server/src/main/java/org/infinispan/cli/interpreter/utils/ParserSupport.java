package org.infinispan.cli.interpreter.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class ParserSupport {
   private static final Map<String, TimeUnit> TIMEUNITS;
   static {
      TIMEUNITS = new HashMap<String, TimeUnit>();
      TIMEUNITS.put("d", TimeUnit.DAYS);
      TIMEUNITS.put("h", TimeUnit.HOURS);
      TIMEUNITS.put("m", TimeUnit.MINUTES);
      TIMEUNITS.put("s", TimeUnit.SECONDS);
      TIMEUNITS.put("ms", TimeUnit.MILLISECONDS);
   }

   /**
    * Converts a time representation into milliseconds
    *
    * @param time
    * @param timeUnit
    * @return
    */
   public static long millis(final String time, final String timeUnit) {
      return TIMEUNITS.get(timeUnit).toMillis(Long.parseLong(time));
   }

   public static long millis(final String time) {
      int s = time.length() - 1;
      for (; time.charAt(s) > '9'; s--) {
      }
      return millis(time.substring(0, s + 1), time.substring(s + 1));
   }

   public static String unquote(final String s) {
      if (s == null || s.length() < 2) {
         return s;
      }
      char first = s.charAt(0);
      char last = s.charAt(s.length() - 1);
      if (first == last && (first == '\'' || first == '"')) {
         return s.substring(1, s.length() - 1);
      } else {
         return s;
      }

   }
}
