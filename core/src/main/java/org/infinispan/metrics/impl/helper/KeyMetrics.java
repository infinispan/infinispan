package org.infinispan.metrics.impl.helper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.commons.stat.CounterTracker;
import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.commons.stat.TimerTracker;
import org.infinispan.distribution.Ownership;
import org.infinispan.metrics.impl.MetricUtils;

public class KeyMetrics<C> {

   private final EnumMap<Metric, KeyMetric> metrics;

   public KeyMetrics() {
      metrics = new EnumMap<>(Metric.class);
      Arrays.stream(Metric.values()).forEach(metric -> metrics.put(metric, new KeyMetric()));
   }

   public List<MetricInfo> getMetrics(boolean histograms, Function<C, KeyMetrics<C>> transformer) {
      var values = Metric.values();
      List<MetricInfo> metrics = new ArrayList<>(values.length * 4);
      for (var v : values) {
         v.addMetricInfo(metrics, histograms, transformer);
      }
      return metrics;
   }

   public void recordMiss(long nanos) {
      recordWithoutOwnership(Metric.MISSES, nanos);
   }

   public void recordMiss(long nanos, Ownership ownership) {
      recordSingle(Metric.MISSES, nanos, ownership);
   }

   public void recordHit(long nanos) {
      recordWithoutOwnership(Metric.HITS, nanos);
   }

   public void recordHit(long nanos, Ownership ownership) {
      recordSingle(Metric.HITS, nanos, ownership);
   }

   public void recordStore(long nanos) {
      recordWithoutOwnership(Metric.STORES, nanos);
   }

   public void recordStore(long nanos, Ownership ownership) {
      recordSingle(Metric.STORES, nanos, ownership);
   }

   public void recordRemoveHit(long nanos) {
      recordWithoutOwnership(Metric.REMOVE_HITS, nanos);
   }

   public void recordRemoveHit(long nanos, Ownership ownership) {
      recordSingle(Metric.REMOVE_HITS, nanos, ownership);
   }

   public void recordRemoveMiss(long nanos, Ownership ownership) {
      recordSingle(Metric.REMOVE_MISSES, nanos, ownership);
   }


   private void recordWithoutOwnership(Metric metric, long nanos) {
      metrics.get(metric).times.update(nanos, TimeUnit.NANOSECONDS);
   }

   private void recordSingle(Metric metric, long nanos, Ownership ownership) {
      var m = metrics.get(metric);
      m.times.update(nanos, TimeUnit.NANOSECONDS);
      switch (ownership) {
         case PRIMARY:
            m.primaryOwner.increment();
            break;
         case BACKUP:
            m.backupOwner.increment();
            break;
         case NON_OWNER:
            m.nonOwner.increment();
            break;
      }
   }

   private enum Metric {
      HITS("Hit", "read hits"),
      MISSES("Miss", "read misses"),
      STORES("Store", "stores"),
      REMOVE_HITS("RemoveHit", "remove hits"),
      REMOVE_MISSES("RemoveMiss", "remove misses");

      final String name;
      final String description;

      Metric(String name, String description) {
         this.name = name;
         this.description = description;
      }

      <C> void addMetricInfo(List<MetricInfo> metrics, boolean histograms, Function<C, KeyMetrics<C>> transformer) {
         metrics.add(MetricUtils.<C>createCounter(name + "PrimaryOwner", "The number of single key " + description + " when this node is the primary owner", (o, t) -> transformer.apply(o).metrics.get(this).primaryOwner = t, null));
         metrics.add(MetricUtils.<C>createCounter(name + "BackupOwner", "The number of single key " + description + " when this node is the backup owner", (o, t) -> transformer.apply(o).metrics.get(this).backupOwner = t, null));
         metrics.add(MetricUtils.<C>createCounter(name + "NonOwner", "The number of single key " + description + " when this node is not an owner", (o, t) -> transformer.apply(o).metrics.get(this).nonOwner = t, null));
         if (histograms) {
            metrics.add(MetricUtils.<C>createTimer(name + "Times", "The " + description + " times", (o, t) -> transformer.apply(o).metrics.get(this).times = t, null));
         } else {
            metrics.add(MetricUtils.<C>createFunctionTimer(name + "Times", "The " + description + " times", (o, t) -> transformer.apply(o).metrics.get(this).times = t, null));
         }
      }
   }

   private static class KeyMetric {
      CounterTracker primaryOwner = CounterTracker.NO_OP;
      CounterTracker backupOwner = CounterTracker.NO_OP;
      CounterTracker nonOwner = CounterTracker.NO_OP;
      TimerTracker times = TimerTracker.NO_OP;
   }

}
