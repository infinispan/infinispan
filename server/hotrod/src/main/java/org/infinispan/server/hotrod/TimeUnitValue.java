package org.infinispan.server.hotrod;

import java.util.concurrent.TimeUnit;

import org.infinispan.util.KeyValuePair;

/**
 * @author wburns
 * @since 9.0
 */
public enum TimeUnitValue {
   SECONDS(0x00),
   MILLISECONDS(0x01),
   NANOSECONDS(0x02),
   MICROSECONDS(0x03),
   MINUTES(0x04),
   HOURS(0x05),
   DAYS(0x06),
   DEFAULT(0x07),
   INFINITE(0x08);

   private final byte code;

   TimeUnitValue(int code) {
      this.code = (byte) code;
   }

   public byte getCode() {
      return code;
   }

   public TimeUnit toTimeUnit() {
      switch (code) {
         case 0x00:
            return TimeUnit.SECONDS;
         case 0x01:
            return TimeUnit.MILLISECONDS;
         case 0x02:
            return TimeUnit.NANOSECONDS;
         case 0x03:
            return TimeUnit.MICROSECONDS;
         case 0x04:
            return TimeUnit.MINUTES;
         case 0x05:
            return TimeUnit.HOURS;
         case 0x06:
            return TimeUnit.DAYS;
         default:
            throw new IllegalArgumentException("TimeUnit not supported for: " + code);
      }
   }

   public static TimeUnitValue decode(byte rightBits) {
      switch (rightBits) {
         case 0x00:
            return SECONDS;
         case 0x01:
            return MILLISECONDS;
         case 0x02:
            return NANOSECONDS;
         case 0x03:
            return MICROSECONDS;
         case 0x04:
            return MINUTES;
         case 0x05:
            return HOURS;
         case 0x06:
            return DAYS;
         case 0x07:
            return DEFAULT;
         case 0x08:
            return INFINITE;
         default:
            throw new IllegalArgumentException("Unsupported byte value: " + rightBits);
      }
   }

   public static KeyValuePair<TimeUnitValue, TimeUnitValue> decodePair(byte timeUnitValues) {
      return new KeyValuePair<>(decode((byte) ((timeUnitValues & 0xf0) >> 4)), decode((byte) (timeUnitValues & 0x0f)));
   }
}
