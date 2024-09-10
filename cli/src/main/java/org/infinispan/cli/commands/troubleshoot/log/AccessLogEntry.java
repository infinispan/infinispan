package org.infinispan.cli.commands.troubleshoot.log;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Represents a single line in an access log file.
 *
 * @param file: The absolute path of the file containing the entry.
 * @param client: The client address which submitted the operation.
 * @param operation: Which operation is performed.
 * @param duration: The operation duration in milliseconds.
 * @param date: The date of the operation.
 */
record AccessLogEntry(String file, String client, String operation, long duration, LocalDateTime date) {
   private static final DateTimeFormatter PARSER = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss");

   /**
    * Creates a new instance representing the provided line.
    * <p>
    * This method creates a new instance to represent the log line. Therefore, it expects the line to have a pre-defined
    * format. The line should follow as:
    * </p>
    *
    * <pre>ip-address - [date-time UTC] "operation /cache-name/key-bytes protocol" status request-size response-size duration-ms</pre>
    *
    * <p>
    * The symbols and spaces are explicit. The `<code>date-time</code>` has to follow the pattern `<code>dd/MMM/yyyy:HH:mm:ss</code>`
    * to parse (e.g. <code>04/Sep/2024:14:35:11</code>). The other parameters do not need a specific format, only the
    * position is required.
    * </p>
    *
    * @param file The file absolute path.
    * @param line A single line in the file, it must follow the expected format.
    * @return A new instance of {@link AccessLogEntry}.
    */
   public static AccessLogEntry newInstance(String file, String line) {
      String[] split = line.split(" ");
      String client = split[0];
      String operation = split[4].replace("\"", "");
      long duration = Long.parseLong(split[split.length - 1]);
      LocalDateTime execution = parseDate(split[2].replace("[", ""));
      return new AccessLogEntry(file, client, operation, duration, execution);
   }

   /**
    * Predicate to accept instances with the operation matching.
    *
    * @param operation Operation to match
    * @return A predicate to filter entries with the given operation.
    */
   public static Predicate<AccessLogEntry> filterByOperation(String operation) {
      return ale -> operation == null || operation.equals(ale.operation);
   }

   /**
    * Predicate to exclude instances with any of the given operations.
    *
    * @param operations Operations to ignore.
    * @return A predicate to ignore if the instance operation match any of the given list.
    */
   public static Predicate<AccessLogEntry> ignoreOperations(List<String> operations) {
      if (operations == null) return ignore -> true;

      Set<String> ops = new HashSet<>(operations);
      return ale -> !ops.contains(ale.operation);
   }

   /**
    * Predicate to accept instance with the execution at and after the given time.
    *
    * @param start The date to filter the instances.
    * @return A predicate to accept instances that happen at or after the given time.
    */
   public static Predicate<AccessLogEntry> filterExecutionAfter(LocalDateTime start) {
      if (start == null) return ignore -> true;

      return ale -> ale.date.equals(start) || ale.date.isAfter(start);
   }

   /**
    * Predicate to accept instance with the execution before the given time.
    *
    * @param end The date to filter the instances.
    * @return A predicate to accept instances that happen before the given time.
    */
   public static Predicate<AccessLogEntry> filterExecutionBefore(LocalDateTime end) {
      if (end == null) return ignore -> true;

      Predicate<AccessLogEntry> predicate = filterExecutionAfter(end);
      return ale -> !predicate.test(ale);
   }

   /**
    * Predicate to accept instances with a duration greater than or equal to a given value.
    *
    * @param d The duration value to compare.
    * @return A predicate to filter the instances by duration.
    */
   public static Predicate<AccessLogEntry> filterByDuration(long d) {
      return ale -> ale.duration >= d;
   }

   private static LocalDateTime parseDate(String value) {
      return PARSER.parse(value, LocalDateTime::from);
   }
}
