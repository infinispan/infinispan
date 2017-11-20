package org.infinispan.client.hotrod.counter;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.server.hotrod.counter.WeakCounterTestStrategy;
import org.infinispan.server.hotrod.counter.impl.WeakCounterImplTestStrategy;
import org.testng.annotations.Test;

/**
 * A {@link WeakCounter} implementation test.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@Test(groups = "functional", testName = "client.hotrod.counter.WeakCounterAPITest")
public class WeakCounterAPITest extends BaseCounterAPITest<WeakCounter> implements WeakCounterTestStrategy {

   private final WeakCounterImplTestStrategy strategy;

   public WeakCounterAPITest() {
      strategy = new WeakCounterImplTestStrategy(this::counterManager, this::counterManagers);
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

   @Override
   public void testListenerAddAndRemove(Method method) throws InterruptedException {
      strategy.testListenerAddAndRemove(method);
   }

   @Override
   void increment(WeakCounter counter) {
      counter.sync().increment();
   }

   @Override
   void add(WeakCounter counter, long delta, long result) {
      strategy.add(counter, delta, result);
   }

   @Override
   WeakCounter defineAndCreateCounter(String counterName, long initialValue) {
      return strategy.defineAndCreateCounter(counterName, initialValue);
   }

   @Override
   <L extends CounterListener> Handle<L> addListenerTo(WeakCounter counter, L logger) {
      return strategy.addListenerTo(counter, logger);
   }

   @Override
   List<WeakCounter> getCounters(String name) {
      return counterManagers().stream()
            .map(counterManager -> counterManager.getWeakCounter(name))
            .collect(Collectors.toList());
   }
}
