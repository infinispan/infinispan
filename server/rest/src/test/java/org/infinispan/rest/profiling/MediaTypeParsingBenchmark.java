package org.infinispan.rest.profiling;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.MediaType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * This benchmark tests the performance of media type parsing
 *
 * @author Dan Berindei
 */
public class MediaTypeParsingBenchmark {

   private static final int MEASUREMENT_ITERATIONS_COUNT = 10;
   private static final int WARMUP_ITERATIONS_COUNT = 10;

   public static void main(String[] args) throws Exception {
      Options opt = new OptionsBuilder()
            .include(MediaTypeParsingBenchmark.class.getName() + ".State.*")
//            .include(MediaTypeParsingBenchmark.class.getName() + ".State.parseOneQuotedParameter")
            .mode(Mode.AverageTime)
            .timeUnit(TimeUnit.NANOSECONDS)
            .warmupIterations(WARMUP_ITERATIONS_COUNT)
            .measurementIterations(MEASUREMENT_ITERATIONS_COUNT)
            .threads(1)
            .forks(3)
            .shouldFailOnError(true)
//            .jvmArgsAppend("-agentpath:/home/dan/Tools/async-profiler/build/libasyncProfiler.so=start,file=profile-%t.svg")
//            .addProfiler("perfasm")
            .build();

      new Runner(opt).run();
   }

   @org.openjdk.jmh.annotations.State(Scope.Benchmark)
   public static class State {
      private final String mediaTypeNoParameter = "application/json";
      private final String mediaTypeOneQuotedParameter = "application/json; charset=\"UTF-8\"";
      private String mediaTypeOneParameter = "application/x-java-object; type=ByteArray";
      private String mediaTypeTwoParameters = "application/x-java-object; q=0.2; type=java.lang.Integer";
      private String mediaTypeList =
            String.join(", ", mediaTypeNoParameter, mediaTypeOneParameter, mediaTypeOneQuotedParameter, mediaTypeTwoParameters);

      @Benchmark
      @OperationsPerInvocation(100)
      public MediaType parseNoParameter() throws Exception {
         return MediaType.fromString(mediaTypeNoParameter);
      }

      @Benchmark
      @OperationsPerInvocation(100)
      public MediaType parseOneParameter() throws Exception {
         return MediaType.fromString(mediaTypeOneParameter);
      }

      @Benchmark
      @OperationsPerInvocation(100)
      public MediaType parseOneQuotedParameter() throws Exception {
         return MediaType.fromString(mediaTypeOneQuotedParameter);
      }

      @Benchmark
      @OperationsPerInvocation(100)
      public MediaType parseTwoParameters() throws Exception {
         return MediaType.fromString(mediaTypeTwoParameters);
      }

      @Benchmark
      @OperationsPerInvocation(100)
      public List<MediaType> parseList() throws Exception {
         return MediaType.parseList(mediaTypeList).collect(Collectors.toList());
      }
   }
}
