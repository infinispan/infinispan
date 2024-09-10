package org.infinispan.cli.commands.troubleshoot.log;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import org.HdrHistogram.Histogram;

/**
 * Wraps the response after parsing the access log files.
 *
 * @since 15.0
 */
interface AccessLogResult {

   /**
    * @return A single string to print the results.
    */
   String prettyPrint();

   /**
    * Result class holding global statistics per operation.
    *
    * <p>
    * This class displays statistics segmented by operation. The values include the min, max, and average (remember
    * that very unlikely the values follow a normal distribution), and percentiles (50, 90, 99).
    * </p>
    */
   final class ResultByOperation implements AccessLogResult {
      private final Map<String, Histogram> results;

      public ResultByOperation(Map<String, Histogram> results) {
         this.results = results;
      }

      @Override
      public String prettyPrint() {
         StringBuilder sb = new StringBuilder();
         for (Map.Entry<String, Histogram> entry : results.entrySet()) {
            Histogram h = entry.getValue();
            sb.append("Operation: ")
                  .append(entry.getKey())
                  .append(" (").append(h.getTotalCount()).append(")")
                  .append(System.lineSeparator());
            sb.append('\t');
            sb.append("min/avg/max: ");

            sb.append(h.getMinValue()).append("/")
                  .append(String.format("%.5f", h.getMean())).append("/")
                  .append(h.getMaxValue())
                  .append(" ms").append(System.lineSeparator());

            sb.append("\tp50: ").append(h.getValueAtPercentile(50)).append(" ms").append(System.lineSeparator());
            sb.append("\tp90: ").append(h.getValueAtPercentile(90)).append(" ms").append(System.lineSeparator());
            sb.append("\tp99: ").append(h.getValueAtPercentile(99)).append(" ms").append(System.lineSeparator());

            sb.append(System.lineSeparator());
         }
         return sb.toString();
      }
   }

   /**
    * Result that holds a collection of entries.
    *
    * <p>
    * This result instance does not show any statistics. Instead, it contains a collection with explicit entries from
    * the access log file.
    * </p>
    */
   final class OperationCollectionResult implements AccessLogResult {

      private final Collection<AccessLogEntry> entries;

      OperationCollectionResult(Collection<AccessLogEntry> entries) {
         this.entries = entries;
      }

      @Override
      public String prettyPrint() {
         StringBuilder sb = new StringBuilder();
         sb.append("Displaying ").append(entries.size()).append(" elements").append(System.lineSeparator());
         sb.append("==============================================================").append(System.lineSeparator());
         String s = entries.stream()
               .map(Record::toString)
               .collect(Collectors.joining(System.lineSeparator()));
         sb.append(s).append(System.lineSeparator());
         return sb.toString();
      }
   }


   /**
    * Result that recursively aggregates other results.
    */
   final class RecursiveAggregationResult implements AccessLogResult {
      private final Map<String, AccessLogResult> results;

      RecursiveAggregationResult(Map<String, AccessLogResult> results) {
         this.results = results;
      }

      @Override
      public String prettyPrint() {
         StringBuilder sb = new StringBuilder();
         sb.append("Total results: ").append(results.size()).append(System.lineSeparator());
         for (Map.Entry<String, AccessLogResult> entry : results.entrySet()) {
            sb.append("Element: ").append(entry.getKey()).append(System.lineSeparator());
            sb.append(entry.getValue().prettyPrint()).append(System.lineSeparator());
            sb.append("==============================================================")
                  .append(System.lineSeparator()).append(System.lineSeparator());
         }
         return sb.toString();
      }
   }
}
