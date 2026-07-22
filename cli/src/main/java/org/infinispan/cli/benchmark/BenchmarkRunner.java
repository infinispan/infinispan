package org.infinispan.cli.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.HdrHistogram.Histogram;
import org.aesh.command.shell.Shell;

public class BenchmarkRunner {

   private static final long HISTOGRAM_MAX = TimeUnit.MINUTES.toNanos(1);

   private final Shell shell;
   private final VerboseMode verbose;
   private final BenchmarkMode mode;
   private final int threads;
   private final int warmupCount;
   private final long warmupTimeNanos;
   private final int measurementCount;
   private final long measurementTimeNanos;
   private final TimeUnit reportUnit;
   private final Supplier<BenchmarkTask> taskFactory;

   public BenchmarkRunner(Shell shell, VerboseMode verbose, BenchmarkMode mode,
                          int threads, int warmupCount, long warmupTimeNanos,
                          int measurementCount, long measurementTimeNanos,
                          TimeUnit reportUnit, Supplier<BenchmarkTask> taskFactory) {
      this.shell = shell;
      this.verbose = verbose;
      this.mode = mode;
      this.threads = threads;
      this.warmupCount = warmupCount;
      this.warmupTimeNanos = warmupTimeNanos;
      this.measurementCount = measurementCount;
      this.measurementTimeNanos = measurementTimeNanos;
      this.reportUnit = reportUnit;
      this.taskFactory = taskFactory;
   }

   public void run() {
      printHeader();

      List<IterationResult> measurementResults = new ArrayList<>();
      Histogram aggregate = new Histogram(HISTOGRAM_MAX, 3);

      BenchmarkTask[] tasks = new BenchmarkTask[threads];
      for (int i = 0; i < threads; i++) {
         tasks[i] = taskFactory.get();
         tasks[i].setup();
      }

      try {
         for (int i = 1; i <= warmupCount; i++) {
            IterationResult r = runIteration(tasks, warmupTimeNanos);
            print(String.format("Warmup    %3d: %s%n", i, formatResult(r)));
         }

         for (int i = 1; i <= measurementCount; i++) {
            IterationResult r = runIteration(tasks, measurementTimeNanos);
            measurementResults.add(r);
            aggregate.add(r.histogram);
            print(String.format("Iteration %3d: %s%n", i, formatResult(r)));
         }

         printSummary(measurementResults, aggregate);
      } finally {
         for (BenchmarkTask task : tasks) {
            try {
               task.teardown();
            } catch (Exception ignored) {
            }
         }
      }
   }

   private IterationResult runIteration(BenchmarkTask[] tasks, long durationNanos) {
      AtomicBoolean running = new AtomicBoolean(true);
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(threads);
      long[] opCounts = new long[threads];
      Histogram[] perThread = new Histogram[threads];

      for (int t = 0; t < threads; t++) {
         final int idx = t;
         final BenchmarkTask task = tasks[idx];
         perThread[idx] = new Histogram(HISTOGRAM_MAX, 3);
         Thread thread = new Thread(() -> {
            try {
               startLatch.await();
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               return;
            }
            long ops = 0;
            Histogram h = perThread[idx];
            while (running.get()) {
               long start = System.nanoTime();
               task.run();
               long elapsed = System.nanoTime() - start;
               h.recordValue(Math.min(elapsed, HISTOGRAM_MAX - 1));
               ops++;
            }
            opCounts[idx] = ops;
            doneLatch.countDown();
         }, "benchmark-" + idx);
         thread.setDaemon(true);
         thread.start();
      }

      startLatch.countDown();
      long iterStart = System.nanoTime();
      try {
         Thread.sleep(TimeUnit.NANOSECONDS.toMillis(durationNanos));
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
      running.set(false);
      try {
         doneLatch.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
      long iterElapsed = System.nanoTime() - iterStart;

      long totalOps = 0;
      Histogram histogram = new Histogram(HISTOGRAM_MAX, 3);
      for (int t = 0; t < threads; t++) {
         totalOps += opCounts[t];
         histogram.add(perThread[t]);
      }

      return new IterationResult(totalOps, iterElapsed, histogram);
   }

   private String formatResult(IterationResult r) {
      switch (mode) {
         case Throughput: {
            double opsPerSec = (double) r.totalOps / ((double) r.elapsedNanos / TimeUnit.SECONDS.toNanos(1));
            double converted = convertThroughput(opsPerSec);
            return String.format("%.3f ops/%s", converted, abbreviate(reportUnit));
         }
         case AverageTime: {
            double avgNanos = r.totalOps > 0 ? (double) r.elapsedNanos / r.totalOps : 0;
            double converted = convertTime(avgNanos);
            return String.format("%.3f %s/op", converted, abbreviate(reportUnit));
         }
         case SampleTime: {
            double p50 = convertTime(r.histogram.getValueAtPercentile(50));
            double p99 = convertTime(r.histogram.getValueAtPercentile(99));
            return String.format("p50 = %.3f %s/op, p99 = %.3f %s/op",
                  p50, abbreviate(reportUnit), p99, abbreviate(reportUnit));
         }
         default:
            return "";
      }
   }

   private void printHeader() {
      if (verbose == VerboseMode.SILENT) return;
      println("# Warmup: " + warmupCount + " iterations, " + formatDuration(warmupTimeNanos) + " each");
      println("# Measurement: " + measurementCount + " iterations, " + formatDuration(measurementTimeNanos) + " each");
      println("# Threads: " + threads + " " + (threads > 1 ? "threads" : "thread"));
      println("# Benchmark mode: " + mode.longLabel());
   }

   private void printSummary(List<IterationResult> results, Histogram aggregate) {
      if (results.isEmpty()) return;
      println("");

      String taskName = taskFactory.get().name();

      switch (mode) {
         case Throughput: {
            double[] throughputs = results.stream().mapToDouble(r ->
                  convertThroughput((double) r.totalOps / ((double) r.elapsedNanos / TimeUnit.SECONDS.toNanos(1)))
            ).toArray();
            double mean = mean(throughputs);
            double stdev = stdev(throughputs, mean);
            println(String.format("Result \"%s\":", taskName));
            println(String.format("  %.3f ±(99.9%%) %.3f ops/%s [Average]", mean, stdev * 3.291, abbreviate(reportUnit)));
            println(String.format("  min = %.3f, avg = %.3f, max = %.3f, stdev = %.3f",
                  min(throughputs), mean, max(throughputs), stdev));
            break;
         }
         case AverageTime: {
            double[] avgTimes = results.stream().mapToDouble(r ->
                  convertTime(r.totalOps > 0 ? (double) r.elapsedNanos / r.totalOps : 0)
            ).toArray();
            double mean = mean(avgTimes);
            double stdev = stdev(avgTimes, mean);
            println(String.format("Result \"%s\":", taskName));
            println(String.format("  %.3f ±(99.9%%) %.3f %s/op [Average]", mean, stdev * 3.291, abbreviate(reportUnit)));
            println(String.format("  min = %.3f, avg = %.3f, max = %.3f, stdev = %.3f",
                  min(avgTimes), mean, max(avgTimes), stdev));
            break;
         }
         case SampleTime: {
            println(String.format("Result \"%s\":", taskName));
            println(String.format("  N = %d", aggregate.getTotalCount()));
            println(String.format("  mean    = %.3f %s/op", convertTime(aggregate.getMean()), abbreviate(reportUnit)));
            println(String.format("  min     = %.3f %s/op", convertTime(aggregate.getMinValue()), abbreviate(reportUnit)));
            println(String.format("  max     = %.3f %s/op", convertTime(aggregate.getMaxValue()), abbreviate(reportUnit)));
            println(String.format("  stdev   = %.3f %s", convertTime(aggregate.getStdDeviation()), abbreviate(reportUnit)));
            println("  percentiles:");
            for (double p : new double[]{50, 90, 95, 99, 99.9, 100}) {
               String label = p == 100 ? "p100 " : p == 99.9 ? "p99.9" : String.format("p%-4.0f", p);
               println(String.format("    %s = %.3f %s/op", label, convertTime(aggregate.getValueAtPercentile(p)), abbreviate(reportUnit)));
            }
            break;
         }
      }
      println("");
   }

   private double convertThroughput(double opsPerSec) {
      switch (reportUnit) {
         case SECONDS: return opsPerSec;
         case MILLISECONDS: return opsPerSec / 1_000.0;
         case MICROSECONDS: return opsPerSec / 1_000_000.0;
         case NANOSECONDS: return opsPerSec / 1_000_000_000.0;
         default: return opsPerSec;
      }
   }

   private double convertTime(double nanos) {
      switch (reportUnit) {
         case SECONDS: return nanos / 1_000_000_000.0;
         case MILLISECONDS: return nanos / 1_000_000.0;
         case MICROSECONDS: return nanos / 1_000.0;
         case NANOSECONDS: return nanos;
         default: return nanos;
      }
   }

   static String abbreviate(TimeUnit unit) {
      switch (unit) {
         case NANOSECONDS: return "ns";
         case MICROSECONDS: return "us";
         case MILLISECONDS: return "ms";
         case SECONDS: return "s";
         default: return unit.name().toLowerCase();
      }
   }

   private static String formatDuration(long nanos) {
      long seconds = TimeUnit.NANOSECONDS.toSeconds(nanos);
      if (seconds > 0) return seconds + "s";
      long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
      return millis + "ms";
   }

   private static double mean(double[] values) {
      double sum = 0;
      for (double v : values) sum += v;
      return sum / values.length;
   }

   private static double stdev(double[] values, double mean) {
      if (values.length < 2) return 0;
      double sum = 0;
      for (double v : values) sum += (v - mean) * (v - mean);
      return Math.sqrt(sum / (values.length - 1));
   }

   private static double min(double[] values) {
      double m = Double.MAX_VALUE;
      for (double v : values) if (v < m) m = v;
      return m;
   }

   private static double max(double[] values) {
      double m = -Double.MAX_VALUE;
      for (double v : values) if (v > m) m = v;
      return m;
   }

   private void print(String s) {
      if (verbose != VerboseMode.SILENT) shell.write(s);
   }

   private void println(String s) {
      if (verbose != VerboseMode.SILENT) shell.writeln(s);
   }

   public static long parseTime(String time) {
      String trimmed = time.trim().toLowerCase();
      long multiplier;
      String number;
      if (trimmed.endsWith("ms")) {
         multiplier = TimeUnit.MILLISECONDS.toNanos(1);
         number = trimmed.substring(0, trimmed.length() - 2);
      } else if (trimmed.endsWith("s")) {
         multiplier = TimeUnit.SECONDS.toNanos(1);
         number = trimmed.substring(0, trimmed.length() - 1);
      } else if (trimmed.endsWith("m")) {
         multiplier = TimeUnit.MINUTES.toNanos(1);
         number = trimmed.substring(0, trimmed.length() - 1);
      } else {
         multiplier = TimeUnit.SECONDS.toNanos(1);
         number = trimmed;
      }
      return Long.parseLong(number) * multiplier;
   }

   static class IterationResult {
      final long totalOps;
      final long elapsedNanos;
      final Histogram histogram;

      IterationResult(long totalOps, long elapsedNanos, Histogram histogram) {
         this.totalOps = totalOps;
         this.elapsedNanos = elapsedNanos;
         this.histogram = histogram;
      }
   }
}
