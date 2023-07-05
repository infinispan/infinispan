package org.infinispan.rest.profiling;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.security.TestCertificates;
import org.infinispan.rest.helper.RestServerHelper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testng.annotations.Test;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;

/**
 * This benchmark checks how faster is HTTP/2 compared to HTTP/1.1
 *
 * @author Sebastian Łaskawiec
 */
@Test(testName = "rest.profiling.Http11VsHttp20Benchmark")
public class Http11VsHttp20Benchmark {

   private static final int MEASUREMENT_ITERATIONS_COUNT = 10;
   private static final int WARMUP_ITERATIONS_COUNT = 10;

   @Test
   public void performHttp11VsHttp20Test() throws Exception {
      Options opt = new OptionsBuilder()
            .include(this.getClass().getName() + ".*")
            .mode(Mode.AverageTime)
            .timeUnit(TimeUnit.MILLISECONDS)
            .warmupIterations(WARMUP_ITERATIONS_COUNT)
            .measurementIterations(MEASUREMENT_ITERATIONS_COUNT)
            .threads(1)
            .forks(1)
            .shouldFailOnError(true)
            .shouldDoGC(true)
            .build();

      new Runner(opt).run();
   }

   @State(Scope.Benchmark)
   public static class BenchmarkState {
      private final String EXISTING_KEY = "existing_key";
      private final String NON_EXISTING_KEY = "non_existing_key";

      @Param({"1", "2", "4", "8"})
      public int httpClientThreads;

      @Param({"true", "false"})
      public boolean useTLS;

      @Param({"true", "false"})
      public boolean useHttp2;

      private RestServerHelper restServer;
      private BenchmarkHttpClient client;

      @Setup
      public void setup() throws Exception {
         //Netty uses SLF and SLF can redirect to all other logging frameworks.
         //Just to make sure we know what we are testing against - let's enforce one of them
         InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
         //Temporary to make the test equal
         System.setProperty("infinispan.server.channel.epoll", "false");

         restServer = RestServerHelper.defaultRestServer();
         if (useTLS) {
            restServer.withKeyStore(TestCertificates.certificate("server"), TestCertificates.KEY_PASSWORD, TestCertificates.KEYSTORE_TYPE);
         }
         restServer.start(this.getClass().getSimpleName());
         restServer.getCacheManager().getCache().put(EXISTING_KEY, "test");

         RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
         builder.addServer().host(restServer.getHost()).port(restServer.getPort());
         if (useTLS) {
            builder.security().ssl().trustStoreFileName(TestCertificates.certificate("ca")).trustStorePassword(TestCertificates.KEY_PASSWORD)
                  .keyStoreFileName(TestCertificates.certificate("client")).keyStorePassword(TestCertificates.KEY_PASSWORD);
         }
         builder.protocol(useHttp2 ? Protocol.HTTP_20 : Protocol.HTTP_11);

         client = new BenchmarkHttpClient(builder.build(), httpClientThreads);
      }

      @TearDown
      public void tearDown() throws Exception {
         restServer.stop();
         client.stop();
      }

      @Benchmark
      @OperationsPerInvocation(100)
      public void measure_put() throws Exception {
         if (useHttp2 && httpClientThreads > 1) {
            return;
         }
         client.performPuts(100);
      }

      @Benchmark
      @OperationsPerInvocation(100)
      public void measure_get() throws Exception {
         if (useHttp2 && httpClientThreads > 1) {
            return;
         }
         client.performGets(0, 100, EXISTING_KEY, NON_EXISTING_KEY);
      }
   }


}
