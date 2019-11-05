package org.infinispan.rest;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;

import org.infinispan.rest.operations.exceptions.WrongDateFormatException;

/**
 * @since 10.0
 */
public final class DateUtils {

   private DateUtils() {
   }

   public static boolean ifUnmodifiedIsBeforeModificationDate(String ifUnmodifiedSince, Long lastMod) {
      if (ifUnmodifiedSince != null && lastMod != null) {
         try {
            Instant instant = Instant.ofEpochSecond(lastMod / 1000);
            ZonedDateTime clientTime = ZonedDateTime.from(DateTimeFormatter.RFC_1123_DATE_TIME.parse(ifUnmodifiedSince));
            ZonedDateTime modificationTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
            return modificationTime.isAfter(clientTime);
         } catch (DateTimeParseException e) {
            throw new WrongDateFormatException("Could not parse date " + ifUnmodifiedSince);
         }
      }
      return false;
   }

   public static ZonedDateTime parseRFC1123(String str) {
      if (str == null) return null;
      try {
         TemporalAccessor temporalAccessor = DateTimeFormatter.RFC_1123_DATE_TIME.parse(str);
         return ZonedDateTime.from(temporalAccessor);
      } catch (DateTimeParseException ex) {
         return null;
      }
   }

   public static String toRFC1123(long epoch) {
      try {
         ZonedDateTime zonedDateTime = Instant.ofEpochMilli(epoch).atZone(ZoneId.systemDefault());
         return DateTimeFormatter.RFC_1123_DATE_TIME.format(zonedDateTime);
      } catch (DateTimeParseException ex) {
         return null;
      }
   }

   public static boolean isNotModifiedSince(String rfc1123Since, Long lasModificationDate) throws WrongDateFormatException {
      if (rfc1123Since == null || lasModificationDate == null) return false;
      try {
         Instant instant = Instant.ofEpochSecond(lasModificationDate / 1000);
         ZonedDateTime lastMod = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
         ZonedDateTime since = ZonedDateTime.from(DateTimeFormatter.RFC_1123_DATE_TIME.parse(rfc1123Since));
         return lastMod.isBefore(since) || lastMod.isEqual(since);
      } catch (DateTimeParseException e) {
         throw new WrongDateFormatException("Could not parse date " + lasModificationDate);
      }
   }
}
