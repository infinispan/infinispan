package org.infinispan.commons.util;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * Parser human-readable quantity of bytes.
 *
 * @since 11.0
 */
public final class ByteQuantity {
   private static final Pattern REGEX_PATTERN = Pattern.compile("^(\\d*\\.?\\d+)\\s*((?:(?:[KMGT]i?)B)|B)?$");
   private static final BigDecimal KILO = new BigDecimal(1000);
   private static final BigDecimal KIBI = new BigDecimal(1024);

   private static final Log log = LogFactory.getLog(ByteQuantity.class);

   /**
    * Parses the byte quantity representation composed of a number plus a unit.
    * When the unit is omitted, it is assumed as bytes (B).
    *
    * The supported units are:
    * <ul>
    *    <li><b>kilobyte (KB)</b>:  1000 bytes</li>
    *    <li><b>megabyte (MB)</b>:  1000<sup>2</sup> bytes</li>
    *    <li><b>gigabyte (GB)</b>:  1000<sup>3</sup> bytes</li>
    *    <li><b>terabyte (TB)</b>:  1000<sup>4</sup> bytes</li>
    *    <li><b>kibibyte (KiB)</b>: 1024 bytes</li>
    *    <li><b>mebibyte (MiB)</b>: 1024<sup>2</sup> bytes</li>
    *    <li><b>gibibyte (GiB)</b>: 1024<sup>3</sup> bytes</li>
    *    <li><b>tebibyte (TiB)</b>: 1024<sup>4</sup> bytes</li>
    * </ul>
    * <p>
    *    Examples: <code>1000</code>,  <code>10 GB</code>,  <code>1.5TB</code>, <code>100 GiB</code>
    * </p>
    *
    * @param str The String representing a quantity (can have decimals) plus the optional unit.
    * @return long number of bytes
    * @throws IllegalArgumentException if the string cannot be parsed.
    *
    */
   public static long parse(String str) throws IllegalArgumentException {
      Matcher matcher = REGEX_PATTERN.matcher(str);
      if (!matcher.find()) throw log.cannotParseQuantity(str);
      try {
         String numberPart = matcher.group(1);
         String unit = matcher.group(2);

         BigDecimal number = new BigDecimal(numberPart);

         if (unit == null) {
            if (numberPart.contains(".")) throw log.cannotParseQuantity(str);
            return number.longValueExact();
         }
         return Unit.valueOf(unit).toBytes(number);
      } catch (ArithmeticException e) {
         throw log.cannotParseQuantity(str);
      }
   }

   private enum Unit {
      B(KILO, 0),
      KB(KILO, 1),
      MB(KILO, 2),
      GB(KILO, 3),
      TB(KILO, 4),
      KiB(KIBI, 1),
      MiB(KIBI, 2),
      GiB(KIBI, 3),
      TiB(KIBI, 4);

      BigDecimal base;
      int exp;

      Unit(BigDecimal base, int exp) {
         this.base = base;
         this.exp = exp;
      }

      long toBytes(BigDecimal quantity) {
         return quantity.multiply(base.pow(exp)).longValueExact();
      }
   }
}
