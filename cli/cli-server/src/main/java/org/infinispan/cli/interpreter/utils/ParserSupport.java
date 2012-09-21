/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
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
