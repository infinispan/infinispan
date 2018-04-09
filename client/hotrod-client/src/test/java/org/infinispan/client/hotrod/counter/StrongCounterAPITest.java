package org.infinispan.client.hotrod.counter;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.server.hotrod.counter.StrongCounterTestStrategy;
import org.infinispan.server.hotrod.counter.impl.StrongCounterImplTestStrategy;
import org.testng.annotations.Test;

/**
 * A {@link StrongCounter} implementation test.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@Test(groups = "functional", testName = "client.hotrod.counter.StrongCounterAPITest")
public class StrongCounterAPITest extends BaseCounterAPITest<StrongCounter> implements StrongCounterTestStrategy {

   private final StrongCounterImplTestStrategy strategy;

   public StrongCounterAPITest() {
      strategy = new StrongCounterImplTestStrategy(this::counterManager, this::counterManagers);
   }

   @Override
   public void testCompareAndSet(Method method) {
      strategy.testCompareAndSet(method);
   }

   @Override
   public void testCompareAndSwap(Method method) {
      strategy.testCompareAndSwap(method);
   }

   @Override
   public void testBoundaries(Method method) {
      strategy.testBoundaries(method);
   }

   @Test(groups = "unstable", description = "ISPN-9053")
   @Override
   public void testListenerWithBounds(Method method) throws InterruptedException {
      strategy.testListenerWithBounds(method);
   }

   @Override
   public void testAdd(Method method) {
      strategy.testAdd(method);
   }

   @Override
   public void testReset(Method method) {
      strategy.testReset(method);
   }

   @Override
   public void testNameAndConfigurationTest(Method method) {
      strategy.testNameAndConfigurationTest(method);
   }

   @Override
   public void testRemove(Method method) {
      strategy.testRemove(method);
   }

   @Test(groups = "unstable", description = "ISPN-9053")
   @Override
   public void testListenerAddAndRemove(Method method) throws InterruptedException {
      strategy.testListenerAddAndRemove(method);
   }

   @Test(groups = "unstable", description = "ISPN-9053")
   @Override
   public void testExceptionInListener(Method method) throws InterruptedException {
      super.testExceptionInListener(method);
   }

   @Test(groups = "unstable", description = "ISPN-9053")
   @Override
   public void testConcurrentListenerAddAndRemove(Method method) throws InterruptedException {
      super.testConcurrentListenerAddAndRemove(method);
   }

   @Test(groups = "unstable", description = "ISPN-9053")
   @Override
   public void testListenerFailover(Method method) throws Exception {
      super.testListenerFailover(method);
   }

   @Override
   void increment(StrongCounter counter) {
      counter.sync().incrementAndGet();
   }

   @Override
   void add(StrongCounter counter, long delta, long result) {
      strategy.add(counter, delta, result);
   }

   @Override
   StrongCounter defineAndCreateCounter(String counterName, long initialValue) {
      return strategy.defineAndCreateCounter(counterName, initialValue);
   }

   @Override
   <L extends CounterListener> Handle<L> addListenerTo(StrongCounter counter, L logger) {
      return strategy.addListenerTo(counter, logger);
   }

   @Override
   List<StrongCounter> getCounters(String name) {
      return counterManagers().stream()
            .map(counterManager -> counterManager.getStrongCounter(name))
            .collect(Collectors.toList());
   }

}
