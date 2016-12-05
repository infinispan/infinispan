package org.infinispan.counter;

import static org.infinispan.counter.EmbeddedCounterManagerFactory.asCounterManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.util.StrongTestCounter;
import org.infinispan.counter.util.TestCounter;
import org.infinispan.counter.util.WeakTestCounter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Stress test for the multiple counter types.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "stress", testName = "counter.CounterStressTest")
public class CounterStressTest extends MultipleCacheManagersTest {

   private static final long TEST_DURATION_MILLIS = TimeUnit.MINUTES.toMillis(2);
   private static final double NANOS_TO_MILLIS = 0.000001;
   private static final double MILLIS_TO_SEC = 0.001;
   private static final int CLUSTER_SIZE = 8;

   private Reports report;

   //input(nanoseconds) => output(milliseconds)
   private static double[] awaitResults(List<Future<Long>> results) throws ExecutionException, InterruptedException {
      double[] millis = new double[results.size()];
      int idx = 0;
      for (Future<Long> result : results) {
         millis[idx++] = result.get() * NANOS_TO_MILLIS;
      }
      return millis;
   }

   private static void printRow(int threads, Result result) {
      double sum = 0;
      double min = Double.MAX_VALUE;
      double max = Double.MIN_VALUE;

      for (double d : result.millis) {
         sum += d;
         min = Math.min(d, min);
         max = Math.max(d, max);
      }

      System.out.printf("%d | %s |", threads, result.factoryName);

      double avg = sum / result.millis.length;
      System.out.printf("avg=%,.2f, min=%,.2f, max=%,.2f |", avg, min, max);

      double avgSec = avg * MILLIS_TO_SEC;
      System.out.printf("%,.2f%n", result.operations / avgSec);
   }

   @BeforeClass(alwaysRun = true)
   public void init() {
      report = new Reports();
   }

   @AfterClass(alwaysRun = true)
   public void report() {
      report.printReport();
   }

   @Test(dataProvider = "threads")
   public void stress(final int threadsPerNode, final TestCounterFactory factory)
         throws ExecutionException, InterruptedException {
      final int threads = threadsPerNode * CLUSTER_SIZE;
      final CyclicBarrier barrier = new CyclicBarrier(threads);
      final List<Future<Long>> results = new ArrayList<>(threads);
      final String counterName = String.format("%s_%d", factory.factoryName(), threadsPerNode);
      final AtomicBoolean stop = new AtomicBoolean(false);

      System.out.println("== STRESS TEST STARTED ==");
      System.out.printf("Factory='%s'%nThreads/Node=%d%nCluster=%d%nCounter name='%s'%n",
            factory.factoryName(), threadsPerNode, CLUSTER_SIZE, counterName);

      for (int c = 0; c < CLUSTER_SIZE; ++c) {
         final TestCounter counter = factory.getCounter(manager(c), counterName);
         for (int t = 0; t < threadsPerNode; ++t) {
            results.add(fork(new StressCallable(counter, barrier, stop)));
         }
      }

      System.out.printf("== THREADS CREATED (%d/%d) ==%n", results.size(), threads);

      Thread.sleep(TEST_DURATION_MILLIS);
      stop.set(true);

      double[] millis = awaitResults(results);

      System.out.println("== STRESS TEST FINISHED ==");

      long[] countersValues = new long[CLUSTER_SIZE];
      for (int c = 0; c < CLUSTER_SIZE; ++c) {
         countersValues[c] = factory.getCounter(manager(c), counterName).getValue();
      }

      this.report.add(threads, factory, millis, countersValues[0]);


      for (int c = 1; c < CLUSTER_SIZE; ++c) {
         AssertJUnit.assertEquals("StrongCounter mismatch for manager " + c, countersValues[0], countersValues[c]);
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().hash().numOwners(2);
      createClusteredCaches(CLUSTER_SIZE, builder);
   }

   @DataProvider(name = "threads")
   private Object[][] threadPerNode() {
      return new Object[][]{
            {2, Factories.ATOMIC},
            {2, Factories.THRESHOLD},
            {2, Factories.WEAK},
            {4, Factories.ATOMIC},
            {4, Factories.THRESHOLD},
            {4, Factories.WEAK},
            {8, Factories.ATOMIC},
            {8, Factories.THRESHOLD},
            {8, Factories.WEAK},
            {16, Factories.ATOMIC},
            {16, Factories.THRESHOLD},
            {16, Factories.WEAK}};
   }

   private enum Factories implements TestCounterFactory {
      ATOMIC {
         @Override
         public TestCounter getCounter(EmbeddedCacheManager manager, String counterName) {
            CounterManager counterManager = asCounterManager(manager);
            counterManager
                  .defineCounter(counterName, CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).build());
            return new StrongTestCounter(counterManager.getStrongCounter(counterName));
         }
      },
      THRESHOLD {
         @Override
         public TestCounter getCounter(EmbeddedCacheManager manager, String counterName) {
            CounterManager counterManager = asCounterManager(manager);
            counterManager.defineCounter(counterName,
                  CounterConfiguration.builder(CounterType.BOUNDED_STRONG).upperBound(Long.MAX_VALUE)
                        .lowerBound(Long.MIN_VALUE)
                        .build());
            return new StrongTestCounter(counterManager.getStrongCounter(counterName));
         }
      },
      WEAK {
         @Override
         public TestCounter getCounter(EmbeddedCacheManager manager, String counterName) {
            CounterManager counterManager = asCounterManager(manager);
            counterManager.defineCounter(counterName, CounterConfiguration.builder(CounterType.WEAK).build());
            return new WeakTestCounter(asCounterManager(manager).getWeakCounter(counterName));
         }
      };

      @Override
      public String factoryName() {
         return name();
      }

   }

   private interface TestCounterFactory {
      TestCounter getCounter(EmbeddedCacheManager manager, String counterName);

      String factoryName();
   }

   private static class Reports {

      private final Map<Integer, List<Result>> reports;

      private Reports() {
         reports = new HashMap<>();
      }

      void add(int threads, TestCounterFactory factory, double[] rawResultsMillis, long operations) {
         List<Result> resultList = reports.computeIfAbsent(threads, t -> new ArrayList<>(3));
         resultList.add(new Result(factory.factoryName(), rawResultsMillis, operations));
      }

      void printReport() {
         System.out.println("== RESULTS ==");
         System.out.println("Threads | Factory | Total Time (ms) | Throughput (op/sec)");
         for (Map.Entry<Integer, List<Result>> entry : reports.entrySet()) {
            int threads = entry.getKey();
            List<Result> results = entry.getValue();
            for (Result result : results) {
               printRow(threads, result);
            }
         }
         System.out.println("== RESULTS ==");
      }

   }

   private static class Result {
      private final String factoryName;
      private final double[] millis;
      private final long operations;

      private Result(String factoryName, double[] millis, long operations) {
         this.factoryName = factoryName;
         this.millis = millis;
         this.operations = operations;
      }
   }

   private class StressCallable implements Callable<Long> {

      private final TestCounter counter;
      private final CyclicBarrier barrier;
      private final AtomicBoolean stop;

      private StressCallable(TestCounter counter, CyclicBarrier barrier, AtomicBoolean stop) {
         this.counter = counter;
         this.barrier = barrier;
         this.stop = stop;
      }

      @Override
      public Long call() throws Exception {
         try {
            barrier.await();
            final long start = System.nanoTime();
            while (!stop.get()) {
               try {
                  counter.increment();
               } catch (Exception e) {
                  log.error("Error incrementing counter.", e);
               }
            }
            final long end = System.nanoTime();
            barrier.await();
            return end - start;
         } catch (Exception e) {
            log.error("Unexpected Exception", e);
            throw e;
         }
      }
   }
}
