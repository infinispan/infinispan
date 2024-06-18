package org.infinispan.commons.util;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.commons.configuration.attributes.AttributeParser;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * Parser human-readable quantity of time.
 *
 * @since 15.1
 */
public class TimeQuantity extends Number implements Matchable<TimeQuantity> {
   private static final TimeQuantity ZERO = new TimeQuantity(null, 0);
   private static final Pattern REGEX_PATTERN = Pattern.compile("^(-?\\d*\\.?\\d+)\\s*(ms|s|m|h|d)?$");
   private static final Log log = LogFactory.getLog(TimeQuantity.class);
   public static AttributeParser<TimeQuantity> PARSER = new TimeQuantityAttributeParser();
   private final String s;
   private final long l;

   private TimeQuantity(String s, long l) {
      this.s = s;
      this.l = l;
   }

   @Override
   public long longValue() {
      return l;
   }

   @Override
   public int intValue() {
      return (int) l;
   }

   @Override
   public float floatValue() {
      return l;
   }

   @Override
   public double doubleValue() {
      return l;
   }

   public Duration toDuration() {
      return Duration.ofMillis(l);
   }

   @Override
   public String toString() {
      return s != null ? s : Long.toString(l);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TimeQuantity that = (TimeQuantity) o;
      return l == that.l;
   }

   @Override
   public int hashCode() {
      return Objects.hashCode(l);
   }

   /**
    * Parses the byte quantity representation composed of a number plus a unit.
    * When the unit is omitted, it is assumed as milliseconds.
    * The supported units are:
    * <ul>
    *    <li><b>ms</b>:  milliseconds</li>
    *    <li><b>s</b>:  seconds</li>
    *    <li><b>m</b>:  minutes</li>
    *    <li><b>h</b>:  hours</li>
    *    <li><b>d</b>:  days</li>
    * </ul>
    * <p>
    *    Examples: <code>1000</code>,  <code>1s</code>,  <code>1.5h</code>, <code>10m</code>
    * </p>
    *
    * @param s The String representing a quantity (can have decimals) plus the optional unit.
    * @return TimeQuantity
    * @throws IllegalArgumentException if the string cannot be parsed.
    */
   public static TimeQuantity valueOf(String s) throws IllegalArgumentException {
      return valueOf(s, 0);
   }

   public static TimeQuantity valueOf(long l) {
      return valueOf(null, l);
   }

   public static TimeQuantity valueOf(String s, long defaultValue) throws IllegalArgumentException {
      if (s == null) {
         return defaultValue == 0 ? ZERO : new TimeQuantity(null, defaultValue);
      }
      Matcher matcher = REGEX_PATTERN.matcher(s);
      if (!matcher.find()) throw log.cannotParseQuantity(s);
      try {
         String numberPart = matcher.group(1);
         String unit = matcher.group(2);
         BigDecimal number = new BigDecimal(numberPart);
         long value;
         if (unit == null) {
            if (numberPart.contains(".")) {
               throw log.cannotParseQuantity(s);
            } else {
               value = number.longValueExact();
            }
         } else {
            value = Unit.valueOf(unit).toMilliseconds(number);
         }
         return new TimeQuantity(s, value);
      } catch (ArithmeticException e) {
         throw log.cannotParseQuantity(s);
      }
   }

   public enum Unit {
      ms(1),
      s(1_000),
      m(60_000),
      h(3_600_000),
      d(86_400_000);

      final BigDecimal factor;

      Unit(long factor) {
         this.factor = BigDecimal.valueOf(factor);
      }

      public long toMilliseconds(BigDecimal quantity) {
         return quantity.multiply(factor).longValue();
      }
   }

   static class TimeQuantityAttributeParser implements AttributeParser<TimeQuantity> {
      @Override
      public TimeQuantity parse(Class<?> klass, String value) {
         return TimeQuantity.valueOf(value);
      }
   }
}
