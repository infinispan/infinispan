package org.infinispan.server.hotrod.counter.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.server.hotrod.counter.BaseCounterTestStrategy;

/**
 * The {@link BaseCounterTestStrategy} implementation.
 *
 * @param <T> the counter implementation ({@link StrongCounter} or {@link WeakCounter})
 * @author Pedro Ruivo
 * @since 9.2
 */
public abstract class BaseCounterImplTest<T> implements BaseCounterTestStrategy {

   final Supplier<CounterManager> counterManagerSupplier;

   BaseCounterImplTest(Supplier<CounterManager> counterManagerSupplier) {
      this.counterManagerSupplier = counterManagerSupplier;
   }

   public static void assertNextValidEvent(Handle<EventLogger> eventLogger, long oldValue, long newValue)
         throws InterruptedException {
      CounterEvent event = eventLogger.getCounterListener().waitingPoll();
      assertValidEvent(event, oldValue, newValue);
   }

   public static void assertValidEvent(CounterEvent event, long oldValue, long newValue) {
      assertNotNull(event);
      assertEquals(CounterState.VALID, event.getOldState());
      assertEquals(CounterState.VALID, event.getNewState());
      assertEquals(oldValue, event.getOldValue());
      assertEquals(newValue, event.getNewValue());
   }

   public static void assertNoEvents(Handle<EventLogger> eventLogger) {
      assertTrue(eventLogger.getCounterListener().eventLog.isEmpty());
   }

   @Override
   public void testAdd(Method method) {
      final String counterName = method.getName();
      final long initialValue = 10;
      T counter = defineAndCreateCounter(counterName, initialValue);
      assertCounterValue(counter, initialValue);

      add(counter, 10, 20);
      assertCounterValue(counter, 20);

      add(counter, -20, 0);
      assertCounterValue(counter, 0);
   }

   @Override
   public void testReset(Method method) {
      final String counterName = method.getName();
      final long initialValue = 5;
      T counter = defineAndCreateCounter(counterName, initialValue);

      add(counter, 100, 105);
      assertCounterValue(counter, 105);

      reset(counter);
      assertCounterValue(counter, initialValue);
   }

   @Override
   public void testNameAndConfigurationTest(Method method) {
      final String prefix = method.getName();
      List<CounterConfiguration> configs = configurationsToTest();

      for (int i = 0; i < configs.size(); ++i) {
         assertTrue(counterManagerSupplier.get().defineCounter(prefix + i, configs.get(i)));
         assertCounterNameAndConfiguration(prefix + i, configs.get(i));
      }
   }

   @Override
   public void testRemove(Method method) {
      final String counterName = method.getName();
      final long initialValue = 5;
      T counter = defineAndCreateCounter(counterName, initialValue);

      add(counter, 100, 105);
      assertCounterValue(counter, 105);

      remove(counter);
      assertCounterValue(counter, initialValue);

      add(counter, -100, -95);
      assertCounterValue(counter, -95);

      counterManagerSupplier.get().remove(counterName);
      assertCounterValue(counter, initialValue);
   }

   @Override
   public void testListenerAddAndRemove(Method method) throws InterruptedException {
      final String prefix = method.getName();
      T counter1 = defineAndCreateCounter(prefix + "1", 0);
      T counter2 = defineAndCreateCounter(prefix + "2", 10);

      Handle<EventLogger> handle1 = addListenerTo(counter1, new EventLogger());
      Handle<EventLogger> handle1_1 = addListenerTo(counter1, new EventLogger());
      Handle<EventLogger> handle2 = addListenerTo(counter2, new EventLogger());

      add(counter1, 1, 1);
      add(counter1, -1, 0);
      add(counter1, 10, 10);
      add(counter2, 1, 11);
      add(counter2, 2, 13);

      assertNextValidEvent(handle1, 0, 1);
      assertNextValidEvent(handle1, 1, 0);
      assertNextValidEvent(handle1, 0, 10);

      assertNextValidEvent(handle1_1, 0, 1);
      assertNextValidEvent(handle1_1, 1, 0);
      assertNextValidEvent(handle1_1, 0, 10);

      assertNextValidEvent(handle2, 10, 11);
      assertNextValidEvent(handle2, 11, 13);

      assertNoEvents(handle1);
      assertNoEvents(handle1_1);
      assertNoEvents(handle2);

      handle1.remove();

      add(counter1, 1, 11);

      assertNextValidEvent(handle1_1, 10, 11);
      assertNoEvents(handle1);

      handle1.remove();
      handle1_1.remove();
      handle2.remove();

      add(counter1, 1, 12);
      add(counter2, 1, 14);

      assertNoEvents(handle1);
      assertNoEvents(handle1_1);
      assertNoEvents(handle2);
   }

   abstract <L extends CounterListener> Handle<L> addListenerTo(T counter, L logger);

   abstract void remove(T counter);

   abstract T defineAndCreateCounter(String counterName, long initialValue);

   abstract void assertCounterValue(T counter, long value);

   abstract void add(T counter, long delta, long result);

   abstract void reset(T counter);

   abstract List<CounterConfiguration> configurationsToTest();

   abstract void assertCounterNameAndConfiguration(String counterName, CounterConfiguration configuration);

   public static class EventLogger implements CounterListener {

      final BlockingQueue<CounterEvent> eventLog;

      public EventLogger() {
         eventLog = new LinkedBlockingQueue<>();
      }

      @Override
      public void onUpdate(CounterEvent entry) {
         eventLog.add(entry);
      }

      public CounterEvent poll() {
         return eventLog.poll();
      }

      public CounterEvent waitingPoll() throws InterruptedException {
         return eventLog.poll(30, TimeUnit.SECONDS);
      }

      public int size() {
         return eventLog.size();
      }
   }
}
