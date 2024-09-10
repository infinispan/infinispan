package org.infinispan.cli.commands.troubleshoot.log;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.HdrHistogram.Histogram;

final class AccessLogSupport {

   private static final long MAX_OPERATION_DURATION = TimeUnit.MINUTES.toMillis(1);

   private AccessLogSupport() { }

   public static AccessLogResult getGlobalStatistics(Stream<AccessLogEntry> source) {
      Map<String, Histogram> statistics = source.reduce(new HashMap<>(), (acc, curr) -> {
         Histogram h = acc.computeIfAbsent(curr.operation(), ignore -> new Histogram(MAX_OPERATION_DURATION, 5));
         h.recordValue(curr.duration());
         return acc;
      }, AccessLogSupport::combineMaps);

      return new AccessLogResult.ResultByOperation(statistics);
   }

   public static AccessLogResult getStatisticsByClient(Stream<AccessLogEntry> source) {
      Map<String, AccessLogResult> results =  source.collect(Collectors.groupingBy(AccessLogEntry::client))
            .entrySet().stream()
            .map(AccessLogSupport::parseNested)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      return new AccessLogResult.RecursiveAggregationResult(results);
   }

   public static AccessLogResult getLongestGlobalOperations(Stream<AccessLogEntry> source, int limit) {
      Collection<AccessLogEntry> entries = source.sorted((a1, a2) -> Long.compare(a2.duration(), a1.duration()))
            .limit(limit)
            .toList();
      return new AccessLogResult.OperationCollectionResult(entries);
   }

   public static AccessLogResult getLongestOperationsByClient(Stream<AccessLogEntry> source, int limit) {
      Map<String, AccessLogResult> results =  source.collect(Collectors.groupingBy(AccessLogEntry::client))
            .entrySet().stream()
            .map(e -> parseNested(e, limit))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      return new AccessLogResult.RecursiveAggregationResult(results);
   }

   private static Map.Entry<String, AccessLogResult> parseNested(Map.Entry<String, List<AccessLogEntry>> entry) {
      return Map.entry(entry.getKey(), getGlobalStatistics(entry.getValue().stream()));
   }

   private static Map.Entry<String, AccessLogResult> parseNested(Map.Entry<String, List<AccessLogEntry>> entry, int limit) {
      return Map.entry(entry.getKey(), getLongestGlobalOperations(entry.getValue().stream(), limit));
   }

   private static Map<String, Histogram> combineMaps(Map<String, Histogram> left, Map<String, Histogram> right) {
      for (Map.Entry<String, Histogram> entry : right.entrySet()) {
         left.compute(entry.getKey(), (ignore, curr) -> {
            if (curr == null) return entry.getValue();

            curr.add(entry.getValue());
            return curr;
         });
      }

      return left;
   }
}
