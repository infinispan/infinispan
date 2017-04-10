package org.infinispan.rest.profiling;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
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

public class RestPerfComparisonTest {

   private static final int MEASUREMENT_ITERATIONS_COUNT = 31;
   private static final int WARMUP_ITERATIONS_COUNT = 1;

   @Test(groups = "profiling")
   public void performRouterBenchmark() throws Exception {
      Options opt = new OptionsBuilder()
            .include(this.getClass().getName() + ".*")
            .mode(Mode.AverageTime)
            .mode(Mode.SingleShotTime)
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

   @State(Scope.Thread)
   public static class BenchmarkState {

      @Param({
            "org.infinispan.rest.profiling.NewServerHandler",
            "org.infinispan.rest.profiling.OldServerHandler",
      })
      public String configurationClassName;

      private EmbeddedCacheManager cacheManager;
      private ServerHandler serverBootstrapHandler;
      private HttpClient httpClient;

      @Setup
      public void setup() throws Exception {
         //Netty uses SLF and SLF can redirect to all other logging frameworks.
         //Just to make sure we know what we are testing against - let's enforce one of them
         InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
         serverBootstrapHandler = (ServerHandler) Class.forName(configurationClassName).getDeclaredConstructor(null).newInstance(null);
         httpClient = new HttpClient();
         httpClient.start();
         cacheManager = new DefaultCacheManager("test-config.xml");
         serverBootstrapHandler.start(cacheManager);
      }

      @TearDown
      public void tearDown() throws Exception {
         serverBootstrapHandler.stop();
         cacheManager.stop();
         httpClient.stop();
      }

      @Benchmark
      public void perform1KPuts() throws Exception {
         for(int i = 0; i < 1_000; ++i) {
            String key = UUID.randomUUID().toString();
            ContentResponse response = httpClient
                  .POST(String.format("http://localhost:%d/rest/%s/%s", 8080, "default", key))
                  .content(new StringContentProvider(key))
                  .header("Content-type", "text/plain")
                  .send();
            if(response.getStatus() != 200) {
               throw new Exception("Status is wrong!");
            }
         }
      }

      @Benchmark
      public void perform1KPutsAndGets() throws Exception {
         for(int i = 0; i < 1_000; ++i) {
            String key = UUID.randomUUID().toString();
            ContentResponse response = httpClient
                  .POST(String.format("http://localhost:%d/rest/%s/%s", 8080, "default", key))
                  .content(new StringContentProvider(key))
                  .header("Content-type", "text/plain")
                  .send();
            if(response.getStatus() != 200) {
               throw new Exception("Status is wrong!");
            }

            response = httpClient
                  .GET(String.format("http://localhost:%d/rest/%s/%s", 8080, "default", key));
            if(response.getStatus() != 200) {
               throw new Exception("Status is wrong!");
            }
         }
      }

   }

}
