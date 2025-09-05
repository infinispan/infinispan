package org.infinispan.query.objectfilter.impl.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * @author anistor@redhat.com
 * @since 8.1
 */
public final class DateHelper {
   public static final DateFormat JPA_DATE_FORMAT;
   public static final DateTimeFormatter JPA_DATETIME_FORMATTER;

   static {
      JPA_DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS");
      JPA_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
      JPA_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
   }

   private DateHelper() {
   }
}
