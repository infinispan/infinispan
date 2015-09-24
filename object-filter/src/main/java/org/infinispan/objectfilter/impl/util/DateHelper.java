package org.infinispan.objectfilter.impl.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * @author anistor@redhat.com
 * @since 8.1
 */
public final class DateHelper {

   private static final String DATE_FORMAT = "yyyyMMddHHmmssSSS";   //todo [anistor] is there a standard jpa time format?

   private static final TimeZone GMT_TZ = TimeZone.getTimeZone("GMT");

   private DateHelper() {
   }

   public static DateFormat getJpaDateFormat() {
      SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
      dateFormat.setTimeZone(GMT_TZ);
      return dateFormat;
   }
}
