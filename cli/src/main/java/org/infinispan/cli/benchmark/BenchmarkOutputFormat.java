package org.infinispan.cli.benchmark;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.aesh.command.shell.Shell;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatFactory;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.IterationType;
import org.openjdk.jmh.runner.format.OutputFormat;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.util.Utils;

public class BenchmarkOutputFormat implements OutputFormat {

   final VerboseMode verbose;
   final Shell out;

   public BenchmarkOutputFormat(Shell out, VerboseMode verbose) {
      this.out = out;
      this.verbose = verbose;
   }

   @Override
   public void print(String s) {
      if (verbose != VerboseMode.SILENT) {
         out.write(s);
      }
   }

   @Override
   public void println(String s) {
      // Hack to filter out unwanted messages
      if (verbose != VerboseMode.SILENT && !s.contains("on-forked runs")) {
         out.writeln(s);
      }
   }

   @Override
   public void flush() {
   }

   @Override
   public void verbosePrintln(String s) {
      if (verbose == VerboseMode.EXTRA) {
         out.writeln(s);
      }
   }

   @Override
   public void write(int b) {
      out.write(new String(Character.toChars(b)));
   }

   @Override
   public void write(byte[] b) {
      // Unused
   }

   @Override
   public void close() {
   }

   @Override
   public void startBenchmark(BenchmarkParams params) {
      if (verbose == VerboseMode.SILENT) {
         return;
      }
      IterationParams warmup = params.getWarmup();
      if (warmup.getCount() > 0) {
         out.writeln("# Warmup: " + warmup.getCount() + " iterations, " +
               warmup.getTime() + " each" +
               (warmup.getBatchSize() <= 1 ? "" : ", " + warmup.getBatchSize() + " calls per op"));
      } else {
         out.writeln("# Warmup: <none>");
      }

      IterationParams measurement = params.getMeasurement();
      if (measurement.getCount() > 0) {
         out.writeln("# Measurement: " + measurement.getCount() + " iterations, " +
               measurement.getTime() + " each" +
               (measurement.getBatchSize() <= 1 ? "" : ", " + measurement.getBatchSize() + " calls per op"));
      } else {
         out.writeln("# Measurement: <none>");
      }

      TimeValue timeout = params.getTimeout();
      boolean timeoutWarning = (timeout.convertTo(TimeUnit.NANOSECONDS) <= measurement.getTime().convertTo(TimeUnit.NANOSECONDS)) ||
            (timeout.convertTo(TimeUnit.NANOSECONDS) <= warmup.getTime().convertTo(TimeUnit.NANOSECONDS));
      out.writeln("# Timeout: " + timeout + " per iteration" + (timeoutWarning ? ", ***WARNING: The timeout might be too low!***" : ""));

      out.write("# Threads: " + params.getThreads() + " " + getThreadsString(params.getThreads()));

      if (!params.getThreadGroupLabels().isEmpty()) {
         int[] tg = params.getThreadGroups();

         List<String> labels = new ArrayList<>(params.getThreadGroupLabels());
         String[] ss = new String[tg.length];
         for (int cnt = 0; cnt < tg.length; cnt++) {
            ss[cnt] = tg[cnt] + "x \"" + labels.get(cnt) + "\"";
         }

         int groupCount = params.getThreads() / Utils.sum(tg);
         out.write(" (" + groupCount + " " + getGroupsString(groupCount) + "; " + Utils.join(ss, ", ") + " in each group)");
      }

      out.writeln(params.shouldSynchIterations() ?
            ", will synchronize iterations" :
            (params.getMode() == Mode.SingleShotTime) ? "" : ", ***WARNING: Synchronize iterations are disabled!***");


      out.writeln("# Benchmark mode: " + params.getMode().longLabel());
      out.writeln("# Benchmark: " + params.getBenchmark());
      if (!params.getParamsKeys().isEmpty()) {
         StringBuilder sb = new StringBuilder();
         boolean isFirst = true;
         for (String k : params.getParamsKeys()) {
            if (isFirst) {
               isFirst = false;
            } else {
               sb.append(", ");
            }
            sb.append(k).append(" = ").append(params.getParam(k));
         }
         out.writeln("# Parameters: (" + sb.toString() + ")");
      }
   }

   @Override
   public void iteration(BenchmarkParams benchmarkParams, IterationParams params, int iteration) {
      if (verbose == VerboseMode.SILENT) {
         return;
      }
      switch (params.getType()) {
         case WARMUP:
            out.write(String.format("Warmup    %3d: ", iteration));
            break;
         case MEASUREMENT:
            out.write(String.format("Iteration %3d: ", iteration));
            break;
         default:
            throw new IllegalStateException("Unknown iteration type: " + params.getType());
      }
      flush();
   }

   protected static String getThreadsString(int t) {
      if (t > 1) {
         return "threads";
      } else {
         return "thread";
      }
   }

   protected static String getGroupsString(int g) {
      if (g > 1) {
         return "groups";
      } else {
         return "group";
      }
   }

   @Override
   public void iterationResult(BenchmarkParams benchmParams, IterationParams params, int iteration, IterationResult data) {
      if (verbose == VerboseMode.SILENT) {
         return;
      }
      StringBuilder sb = new StringBuilder();
      sb.append(data.getPrimaryResult().toString());

      if (params.getType() == IterationType.MEASUREMENT) {
         int prefixLen = String.format("Iteration %3d: ", iteration).length();

         Map<String, Result> secondary = data.getSecondaryResults();
         if (!secondary.isEmpty()) {
            sb.append("\n");

            int maxKeyLen = 0;
            for (Map.Entry<String, Result> res : secondary.entrySet()) {
               maxKeyLen = Math.max(maxKeyLen, res.getKey().length());
            }

            for (Map.Entry<String, Result> res : secondary.entrySet()) {
               sb.append(String.format("%" + prefixLen + "s", ""));
               sb.append(String.format("  %-" + (maxKeyLen + 1) + "s %s", res.getKey() + ":", res.getValue()));
               sb.append("\n");
            }
         }
      }

      out.write(String.format("%s%n", sb));
      flush();
   }

   @Override
   public void endBenchmark(BenchmarkResult result) {
      out.writeln("");
      if (result != null) {
         {
            Result r = result.getPrimaryResult();
            String s = r.extendedInfo();
            if (!s.trim().isEmpty()) {
               out.writeln("Result \"" + result.getParams().getBenchmark() + "\":");
               out.writeln(s);
            }
         }
         for (Result r : result.getSecondaryResults().values()) {
            String s = r.extendedInfo();
            if (!s.trim().isEmpty()) {
               out.writeln("Secondary result \"" + result.getParams().getBenchmark() + ":" + r.getLabel() + "\":");
               out.writeln(s);
            }
         }
         out.writeln("");
      }
   }

   @Override
   public void startRun() {
      // do nothing
   }

   @Override
   public void endRun(Collection<RunResult> runResults) {
      try {
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         PrintStream printStream = new PrintStream(baos, true, StandardCharsets.UTF_8.name());
         ResultFormatFactory.getInstance(ResultFormatType.TEXT, printStream).writeOut(runResults);
         println(baos.toString());
      } catch (UnsupportedEncodingException e) {
         // Not going to happen
      }
   }

}
