package org.infinispan.query.functions.builtin;

import static org.infinispan.query.functions.builtin.FunctionArgs.requireArity;
import static org.infinispan.query.functions.builtin.FunctionArgs.toLong;

import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.infinispan.query.functions.QueryFunction;
import org.infinispan.query.functions.QueryFunctionContext;
import org.infinispan.query.functions.QueryFunctionSignature;

public final class DateFunctions {

   private static final Map<String, ChronoUnit> DATE_UNITS = Map.of(
         "SECOND", ChronoUnit.SECONDS,
         "MINUTE", ChronoUnit.MINUTES,
         "HOUR", ChronoUnit.HOURS,
         "DAY", ChronoUnit.DAYS,
         "WEEK", ChronoUnit.WEEKS,
         "MONTH", ChronoUnit.MONTHS,
         "YEAR", ChronoUnit.YEARS);

   private DateFunctions() {
   }

   public static final class AddDays implements QueryFunction {
      @QueryFunctionSignature(args = {Object.class, Number.class}, returns = Object.class, category = QueryFunctionSignature.DATE)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         return addDateAmount("addDays", args, ChronoUnit.DAYS, false);
      }
   }

   public static final class AddHours implements QueryFunction {
      @QueryFunctionSignature(args = {Object.class, Number.class}, returns = Object.class, category = QueryFunctionSignature.DATE)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         return addDateAmount("addHours", args, ChronoUnit.HOURS, false);
      }
   }

   public static final class AddMinutes implements QueryFunction {
      @QueryFunctionSignature(args = {Object.class, Number.class}, returns = Object.class, category = QueryFunctionSignature.DATE)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         return addDateAmount("addMinutes", args, ChronoUnit.MINUTES, false);
      }
   }

   public static final class AddMonths implements QueryFunction {
      @QueryFunctionSignature(args = {Object.class, Number.class}, returns = Object.class, category = QueryFunctionSignature.DATE)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         return addDateAmount("addMonths", args, ChronoUnit.MONTHS, true);
      }
   }

   public static final class AddYears implements QueryFunction {
      @QueryFunctionSignature(args = {Object.class, Number.class}, returns = Object.class, category = QueryFunctionSignature.DATE)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         return addDateAmount("addYears", args, ChronoUnit.YEARS, true);
      }
   }

   public static final class DateAdd implements QueryFunction {
      @QueryFunctionSignature(args = {Object.class, Number.class, String.class}, returns = Object.class, category = QueryFunctionSignature.DATE)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         requireArity("dateAdd", args, 3);
         Object unit = args.get(2);
         if (unit == null) {
            return null;
         }
         String unitStr = String.valueOf(unit).toUpperCase(Locale.ROOT);
         if (unitStr.endsWith("S")) {
            unitStr = unitStr.substring(0, unitStr.length() - 1);
         }
         ChronoUnit chronoUnit = DATE_UNITS.get(unitStr);
         if (chronoUnit == null) {
            throw new IllegalArgumentException("dateAdd: unknown unit '" + unit + "', expected one of " + DATE_UNITS.keySet());
         }
         return addDateAmount("dateAdd", args, chronoUnit,
               chronoUnit == ChronoUnit.MONTHS || chronoUnit == ChronoUnit.YEARS);
      }
   }

   static Object addDateAmount(String name, List<Object> args, ChronoUnit unit, boolean monthsOrYears) {
      Object date = args.get(0);
      Object amount = args.get(1);
      if (date == null || amount == null) {
         return null;
      }
      long n = toLong(name, amount, 1);
      if (date instanceof Temporal temporal) {
         // Covers all java.time types: Instant, LocalDateTime, LocalDate, LocalTime,
         // ZonedDateTime, OffsetDateTime, OffsetTime, Year, YearMonth. plus() is
         // calendar-aware (correct for months/years) and returns the same concrete type.
         return temporal.plus(n, unit);
      }
      if (date instanceof Date d) {
         if (!monthsOrYears) {
            return new Date(d.getTime() + unit.getDuration().toMillis() * n);
         }
         Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
         c.setTime(d);
         c.add(unit == ChronoUnit.MONTHS ? Calendar.MONTH : Calendar.YEAR, (int) n);
         return c.getTime();
      }
      if (date instanceof Calendar cal) {
         Calendar c = (Calendar) cal.clone();
         c.add(toCalendarField(unit), (int) n);
         return c;
      }
      if (date instanceof Number num) {
         long millis = num.longValue();
         if (!monthsOrYears) {
            return millis + unit.getDuration().toMillis() * n;
         }
         Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
         c.setTimeInMillis(millis);
         c.add(unit == ChronoUnit.MONTHS ? Calendar.MONTH : Calendar.YEAR, (int) n);
         return c.getTimeInMillis();
      }
      throw new IllegalArgumentException("Function " + name + " expects a date/time argument "
            + "(java.util.Date, java.util.Calendar or a java.time type) but got " + date.getClass().getSimpleName());
   }

   private static int toCalendarField(ChronoUnit unit) {
      return switch (unit) {
         case SECONDS -> Calendar.SECOND;
         case MINUTES -> Calendar.MINUTE;
         case HOURS -> Calendar.HOUR;
         case DAYS -> Calendar.DAY_OF_MONTH;
         case WEEKS -> Calendar.WEEK_OF_YEAR;
         case MONTHS -> Calendar.MONTH;
         case YEARS -> Calendar.YEAR;
         default -> throw new IllegalArgumentException("Unsupported date unit: " + unit);
      };
   }
}
