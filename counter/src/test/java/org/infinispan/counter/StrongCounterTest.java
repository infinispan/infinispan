package org.infinispan.counter;

import static java.lang.String.format;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.util.StrongTestCounter;
import org.testng.annotations.Test;

/**
 * A simple consistency test for {@link org.infinispan.counter.api.StrongCounter}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "counter.StrongCounterTest")
public class StrongCounterTest extends AbstractCounterTest<StrongTestCounter> {

   private static final int CLUSTER_SIZE = 4;

   public void testUniqueReturnValues(Method method) throws ExecutionException, InterruptedException, TimeoutException {
      //local mode will have 8 concurrent thread, cluster mode will have 8 concurrent threads (4 nodes, 2 threads per node)
      final int numThreadsPerNode = clusterSize() == 1 ? 8 : 2;
      final int totalThreads = clusterSize() * numThreadsPerNode;
      final List<Future<List<Long>>> workers = new ArrayList<>(totalThreads);
      final String counterName = method.getName();
      final CyclicBarrier barrier = new CyclicBarrier(totalThreads);
      final long counterLimit = 1000;

      for (int i = 0; i < totalThreads; ++i) {
         final int cmIndex = i % clusterSize();
         workers.add(fork(() -> {
            List<Long> retValues = new LinkedList<>();
            CounterManager manager = counterManager(cmIndex);
            StrongTestCounter counter = createCounter(manager, counterName, 0);
            long lastRet = 0;
            barrier.await();

            while (lastRet < counterLimit) {
               lastRet = counter.addAndGet(1);
               retValues.add(lastRet);
            }

            return retValues;
         }));
      }

      final Set<Long> uniqueValuesCheck = new HashSet<>();
      for (Future<List<Long>> w : workers) {
         List<Long> returnValues = w.get(1, TimeUnit.MINUTES);
         for (Long l : returnValues) {
            assertTrue(format("Duplicated value %d", l), uniqueValuesCheck.add(l));
         }
      }
      for (long l = 1; l < (counterLimit + 3); ++l) {
         assertTrue(format("Value %d does not exists!", l), uniqueValuesCheck.contains(l));
      }
   }

   public void testCompareAndSet(Method method) {
      final String counterName = method.getName();
      TestContext context = new TestContext();
      context.printSeed(counterName);

      long expect = context.random.nextLong();
      long value = context.random.nextLong();

      StrongTestCounter counter = createCounter(counterManager(0), counterName, expect);
      for (int i = 0; i < 10; ++i) {
         assertTrue(counter.compareAndSet(expect, value));
         assertEquals(value, counter.getValue());
         expect = value;
         value = context.random.nextLong();
      }

      for (int i = 0; i < 10; ++i) {
         long notExpected = context.random.nextLong();
         while (expect == notExpected) {
            notExpected = context.random.nextLong();
         }
         assertFalse(counter.compareAndSet(notExpected, value));
         assertEquals(expect, counter.getValue());
      }
   }

   public void testCompareAndSwap(Method method) {
      final String counterName = method.getName();
      TestContext context = new TestContext();
      context.printSeed(counterName);

      long expect = context.random.nextLong();
      long value = context.random.nextLong();

      StrongTestCounter counter = createCounter(counterManager(0), counterName, expect);
      for (int i = 0; i < 10; ++i) {
         assertEquals(expect, counter.compareAndSwap(expect, value));
         assertEquals(value, counter.getValue());
         expect = value;
         value = context.random.nextLong();
      }

      for (int i = 0; i < 10; ++i) {
         long notExpected = context.random.nextLong();
         while (expect == notExpected) {
            notExpected = context.random.nextLong();
         }
         assertEquals(expect, counter.compareAndSwap(notExpected, value));
         assertEquals(expect, counter.getValue());
      }
   }

   public void testCompareAndSetConcurrent(Method method)
         throws ExecutionException, InterruptedException, TimeoutException {
      //local mode will have 8 concurrent thread, cluster mode will have 8 concurrent threads (4 nodes, 2 threads per node)
      final int numThreadsPerNode = clusterSize() == 1 ? 8 : 2;
      final int totalThreads = clusterSize() * numThreadsPerNode;
      final List<Future<Boolean>> workers = new ArrayList<>(totalThreads);
      final String counterName = method.getName();
      final CyclicBarrier barrier = new CyclicBarrier(totalThreads);
      final AtomicIntegerArray retValues = new AtomicIntegerArray(totalThreads);
      final long maxIterations = 100;
      for (int i = 0; i < totalThreads; ++i) {
         final int threadIndex = i;
         final int cmIndex = i % clusterSize();
         workers.add(fork(() -> {
            long iteration = 0;
            final long initialValue = 0;
            long previousValue = initialValue;
            CounterManager manager = counterManager(cmIndex);
            StrongTestCounter counter = createCounter(manager, counterName, initialValue);
            while (iteration < maxIterations) {
               assertEquals(previousValue, counter.getValue());
               long update = previousValue + 1;
               barrier.await();
               //all threads calling compareAndSet at the same time, only one should succeed
               boolean ret = counter.compareAndSet(previousValue, update);
               if (ret) {
                  previousValue = update;
               } else {
                  previousValue = counter.getValue();
               }
               retValues.set(threadIndex, ret ? 1 : 0);
               barrier.await();
               assertUnique(retValues, iteration);
               ++iteration;
            }
            return true;
         }));
      }

      for (Future<?> w : workers) {
         w.get(1, TimeUnit.MINUTES);
      }
   }

   public void testCompareAndSwapConcurrent(Method method)
         throws ExecutionException, InterruptedException, TimeoutException {
      //local mode will have 8 concurrent thread, cluster mode will have 8 concurrent threads (4 nodes, 2 threads per node)
      final int numThreadsPerNode = clusterSize() == 1 ? 8 : 2;
      final int totalThreads = clusterSize() * numThreadsPerNode;
      final List<Future<Boolean>> workers = new ArrayList<>(totalThreads);
      final String counterName = method.getName();
      final CyclicBarrier barrier = new CyclicBarrier(totalThreads);
      final AtomicIntegerArray retValues = new AtomicIntegerArray(totalThreads);
      final long maxIterations = 100;
      for (int i = 0; i < totalThreads; ++i) {
         final int threadIndex = i;
         final int cmIndex = i % clusterSize();
         workers.add(fork(() -> {
            long iteration = 0;
            final long initialValue = 0;
            long previousValue = initialValue;
            CounterManager manager = counterManager(cmIndex);
            StrongTestCounter counter = createCounter(manager, counterName, initialValue);
            while (iteration < maxIterations) {
               assertEquals(previousValue, counter.getValue());
               long update = previousValue + 1;
               barrier.await();
               //all threads calling compareAndSet at the same time, only one should succeed
               long ret = counter.compareAndSwap(previousValue, update);
               boolean success = ret == previousValue;
               previousValue = success ? update : ret;
               retValues.set(threadIndex, success ? 1 : 0);
               barrier.await();
               assertUnique(retValues, iteration);
               ++iteration;
            }
            return true;
         }));
      }

      for (Future<?> w : workers) {
         w.get(1, TimeUnit.MINUTES);
      }
   }

   public void testCompareAndSetMaxAndMinLong(Method method) {
      final String counterName = method.getName();
      StrongTestCounter counter = createCounter(counterManager(0), counterName, 0);
      assertFalse(counter.compareAndSet(-1, Long.MAX_VALUE));
      assertEquals(0, counter.getValue());
      assertTrue(counter.compareAndSet(0, Long.MAX_VALUE));
      assertEquals(Long.MAX_VALUE, counter.getValue());
      counter.reset();
      assertFalse(counter.compareAndSet(-1, Long.MIN_VALUE));
      assertEquals(0, counter.getValue());
      assertTrue(counter.compareAndSet(0, Long.MIN_VALUE));
      assertEquals(Long.MIN_VALUE, counter.getValue());
   }

   public void testCompareAndSwapMaxAndMinLong(Method method) {
      final String counterName = method.getName();
      StrongTestCounter counter = createCounter(counterManager(0), counterName, 0);
      assertEquals(0, counter.compareAndSwap(-1, Long.MAX_VALUE));
      assertEquals(0, counter.getValue());
      assertEquals(0, counter.compareAndSwap(0, Long.MAX_VALUE));
      assertEquals(Long.MAX_VALUE, counter.getValue());
      counter.reset();
      assertEquals(0, counter.compareAndSwap(-1, Long.MIN_VALUE));
      assertEquals(0, counter.getValue());
      assertEquals(0, counter.compareAndSwap(0, Long.MIN_VALUE));
      assertEquals(Long.MIN_VALUE, counter.getValue());
   }

   @Override
   protected StrongTestCounter createCounter(CounterManager counterManager, String counterName, long initialValue) {
      counterManager.defineCounter(counterName,
            CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).initialValue(initialValue).build());
      return new StrongTestCounter(counterManager.getStrongCounter(counterName));
   }

   @Override
   protected void assertMaxValueAfterMaxValue(StrongTestCounter counter, long delta) {
      assertEquals(Long.MAX_VALUE, counter.addAndGet(delta));
      assertEquals(Long.MAX_VALUE, counter.getValue());
   }

   @Override
   protected void addAndAssertResult(StrongTestCounter counter, long delta, long expected) {
      assertEquals(format("Wrong return value after adding %d", delta), expected, counter.addAndGet(delta));
      assertEquals("Wrong return value of counter.getNewValue()", expected, counter.getValue());
   }

   @Override
   protected StrongTestCounter createCounter(CounterManager counterManager, String counterName,
         CounterConfiguration configuration) {
      counterManager.defineCounter(counterName, configuration);
      return new StrongTestCounter(counterManager.getStrongCounter(counterName));
   }

   @Override
   protected List<CounterConfiguration> configurationToTest() {
      return Arrays.asList(
            CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).initialValue(10).build(),
            CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).initialValue(20).build(),
            CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).build()
      );
   }

   @Override
   protected void assertMinValueAfterMinValue(StrongTestCounter counter, long delta) {
      assertEquals(Long.MIN_VALUE, counter.addAndGet(delta));
      assertEquals(Long.MIN_VALUE, counter.getValue());
   }

   @Override
   protected int clusterSize() {
      return CLUSTER_SIZE;
   }

   private void assertUnique(AtomicIntegerArray retValues, long it) {
      int successCount = 0;
      for (int ix = 0; ix != retValues.length(); ++ix) {
         successCount += retValues.get(ix);
      }
      assertEquals("Multiple threads succeeded with update in iteration " + it, 1, successCount);
   }
}
