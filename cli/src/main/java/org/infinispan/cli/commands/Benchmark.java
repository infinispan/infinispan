package org.infinispan.cli.commands;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.benchmark.BenchmarkOutputFormat;
import org.infinispan.cli.benchmark.HotRodBenchmark;
import org.infinispan.cli.benchmark.HttpBenchmark;
import org.infinispan.cli.benchmark.RespBenchmark;
import org.infinispan.cli.completers.BenchmarkModeCompleter;
import org.infinispan.cli.completers.BenchmarkVerbosityModeCompleter;
import org.infinispan.cli.completers.BookmarkCompleter;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.completers.TimeUnitCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "benchmark", description = "Benchmarks server performance")
public class Benchmark extends CliCommand {
   @Argument(description = "Specifies the URI of the server to benchmark or a bookmark name. Supported protocols are http, https, hotrod, hotrods, redis, rediss. If you do not set a protocol, the benchmark uses the URI of the current connection.", completer = BookmarkCompleter.class)
   String uri;

   @Option(shortName = 't', defaultValue = "10", description = "Specifies the number of threads to create. Defaults to 10.")
   int threads;


   @Option(completer = BenchmarkModeCompleter.class, defaultValue = "Throughput", description = "Specifies the benchmark mode. Possible values are Throughput, AverageTime, SampleTime, SingleShotTime, and All. Defaults to Throughput.")
   String mode;

   @Option(completer = BenchmarkVerbosityModeCompleter.class, defaultValue = "NORMAL", description = "Specifies the verbosity level of the output. Possible values, from least to most verbose, are SILENT, NORMAL, and EXTRA. Defaults to NORMAL.")
   String verbosity;

   @Option(shortName = 'c', defaultValue = "5", description = "Specifies how many measurement iterations to perform. Defaults to 5.")
   int count;

   @Option(defaultValue = "10s", description = "Sets the amount of time, in seconds, that each iteration takes. Defaults to 10.")
   String time;

   @Option(defaultValue = "5", name = "warmup-count", description = "Specifies how many warmup iterations to perform. Defaults to 5.")
   int warmupCount;

   @Option(defaultValue = "1s", name = "warmup-time", description = "Sets the amount of time, in seconds, that each warmup iteration takes. Defaults to 1.")
   String warmupTime;

   @Option(completer = TimeUnitCompleter.class, defaultValue = "MICROSECONDS", name = "time-unit", description = "Specifies the time unit for results in the benchmark report. Possible values are NANOSECONDS, MICROSECONDS, MILLISECONDS, and SECONDS. The default is MICROSECONDS.")
   String timeUnit;

   @Option(completer = CacheCompleter.class, defaultValue = "benchmark", description = "Names the cache against which the benchmark is performed. Defaults to 'benchmark'.")
   String cache;

   @Option(defaultValue = "16", name = "key-size", description = "Sets the size, in bytes, of the key. Defaults to 16 bytes.")
   int keySize;

   @Option(defaultValue = "1000", name = "value-size", description = "Sets the size, in bytes, of the value. Defaults to 1000 bytes.")
   int valueSize;

   @Option(defaultValue = "1000", name = "keyset-size", description = "Defines the size, in bytes, of the test key set. Defaults to 1000.")
   int keySetSize;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      OptionsBuilder opt = new OptionsBuilder();
      if (this.uri == null) {
         if (invocation.getContext().isConnected()) {
            this.uri = invocation.getContext().connection().getURI();
         } else {
            throw new IllegalArgumentException("You must specify a URI");
         }
      }
      // Resolve bookmark if the argument is not a URI
      if (!this.uri.contains("://")) {
         Bookmark.ResolvedBookmark bookmark = Bookmark.resolve(invocation, this.uri);
         if (bookmark == null) {
            throw new IllegalArgumentException("Bookmark '" + this.uri + "' not found and argument is not a valid URI");
         }
         this.uri = embedCredentials(bookmark);
      }
      URI uri = URI.create(this.uri);
      switch (uri.getScheme()) {
         case "hotrod":
         case "hotrods":
            opt.include(HotRodBenchmark.class.getSimpleName());
            break;
         case "http":
         case "https":
            opt.include(HttpBenchmark.class.getSimpleName());
            break;
         case "redis":
         case "rediss":
            opt.include(RespBenchmark.class.getSimpleName());
            break;
         default:
            throw new IllegalArgumentException("Unknown scheme " + uri.getScheme());
      }
      opt
            .forks(0)
            .threads(threads)
            .param("uri", this.uri)
            .param("cacheName", this.cache)
            .param("keySize", Integer.toString(this.keySize))
            .param("valueSize", Integer.toString(this.valueSize))
            .param("keySetSize", Integer.toString(this.keySetSize))
            .mode(Mode.valueOf(mode))
            .verbosity(VerboseMode.valueOf(verbosity))
            .measurementIterations(count)
            .measurementTime(TimeValue.fromString(time))
            .warmupIterations(warmupCount)
            .warmupTime(TimeValue.fromString(warmupTime))
            .timeUnit(TimeUnit.valueOf(timeUnit));
      try {
         new Runner(opt.build(), new BenchmarkOutputFormat(invocation.getShell(), VerboseMode.valueOf(verbosity))).run();
         return CommandResult.SUCCESS;
      } catch (RunnerException e) {
         throw new CommandException(e);
      }
   }

   /**
    * Builds a URI string from a resolved bookmark, embedding credentials in the URI if present.
    */
   private static String embedCredentials(Bookmark.ResolvedBookmark bookmark) {
      if (bookmark.username() == null) {
         return bookmark.url();
      }
      URI base = URI.create(bookmark.url());
      StringBuilder sb = new StringBuilder();
      sb.append(base.getScheme()).append("://");
      sb.append(bookmark.username());
      if (bookmark.password() != null) {
         sb.append(':').append(bookmark.password());
      }
      sb.append('@');
      sb.append(base.getHost());
      if (base.getPort() > 0) {
         sb.append(':').append(base.getPort());
      }
      if (base.getPath() != null && !base.getPath().isEmpty()) {
         sb.append(base.getPath());
      }
      return sb.toString();
   }
}
