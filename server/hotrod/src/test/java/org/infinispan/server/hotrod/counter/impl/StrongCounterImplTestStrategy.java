package org.infinispan.server.hotrod.counter.impl;

import static java.util.Arrays.asList;
import static org.infinispan.counter.api.CounterConfiguration.builder;
import static org.infinispan.counter.api.CounterType.BOUNDED_STRONG;
import static org.infinispan.counter.api.CounterType.UNBOUNDED_STRONG;
import static org.infinispan.counter.impl.Util.awaitCounterOperation;
import static org.infinispan.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.server.hotrod.counter.StrongCounterTestStrategy;

/**
 * The {@link StrongCounterTestStrategy} implementation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class StrongCounterImplTestStrategy extends BaseCounterImplTest<StrongCounter> implements
      StrongCounterTestStrategy {

   private final Supplier<Collection<CounterManager>> allCounterManagerSupplier;

   public StrongCounterImplTestStrategy(Supplier<CounterManager> counterManagerSupplier,
         Supplier<Collection<CounterManager>> allCounterManagerSupplier) {
      super(counterManagerSupplier);
      this.allCounterManagerSupplier = allCounterManagerSupplier;
   }

   @Override
   public void testCompareAndSet(Method method) {
      final String counterName = method.getName();
      final CounterManager counterManager = counterManagerSupplier.get();

      assertTrue(counterManager.defineCounter(counterName, builder(UNBOUNDED_STRONG).initialValue(2).build()));
      StrongCounter counter = counterManager.getStrongCounter(counterName);

      assertFalse(awaitCounterOperation(counter.compareAndSet(0, 1)));
      assertTrue(awaitCounterOperation(counter.compareAndSet(2, 3)));
   }

   @Override
   public void testCompareAndSwap(Method method) {
      final String counterName = method.getName();
      final CounterManager counterManager = counterManagerSupplier.get();

      assertTrue(counterManager.defineCounter(counterName, builder(UNBOUNDED_STRONG).initialValue(3).build()));
      StrongCounter counter = counterManager.getStrongCounter(counterName);

      assertEquals(3, (long) awaitCounterOperation(counter.compareAndSwap(0, 1)));
      assertEquals(3, (long) awaitCounterOperation(counter.compareAndSwap(3, 2)));
   }

   @Override
   public void testBoundaries(Method method) {
      final String counterName = method.getName();
      final CounterManager counterManager = counterManagerSupplier.get();

      assertTrue(counterManager
            .defineCounter(counterName, builder(BOUNDED_STRONG).initialValue(1).lowerBound(0).upperBound(20).build()));
      StrongCounter counter = counterManager.getStrongCounter(counterName);

      assertCounterValue(counter, 1);

      expectException(ExecutionException.class, CounterOutOfBoundsException.class,
            () -> counter.addAndGet(-10).get());
      expectException(ExecutionException.class, CounterOutOfBoundsException.class,
            () -> counter.addAndGet(30).get());


      assertCounterValue(counter, 20);
      expectException(ExecutionException.class, CounterOutOfBoundsException.class,
            () -> counter.compareAndSet(20, -1).get());

      assertCounterValue(counter, 20);
      expectException(ExecutionException.class, CounterOutOfBoundsException.class,
            () -> counter.compareAndSet(20, 21).get());
   }

   @Override
   public void testListenerWithBounds(Method method) throws InterruptedException {
      final String counterName = method.getName();
      final CounterManager counterManager = counterManagerSupplier.get();
      assertTrue(counterManager
            .defineCounter(counterName, builder(BOUNDED_STRONG).initialValue(0).lowerBound(0).upperBound(20).build()));
      StrongCounter counter = counterManager.getStrongCounter(counterName);
      Handle<EventLogger> handle = counter.addListener(new EventLogger());
      add(counter, 1, 1);
      expectException(ExecutionException.class, CounterOutOfBoundsException.class,
            () -> counter.addAndGet(20).get());
      assertCounterValue(counter, 20);

      reset(counter);
      expectException(CounterOutOfBoundsException.class, () -> add(counter, -1, 0));
      assertCounterValue(counter, 0);

      assertNextValidEvent(handle, 0, 1);
      assertNextEvent(handle, 1, CounterState.VALID, 20, CounterState.UPPER_BOUND_REACHED);
      assertNextEvent(handle, 20, CounterState.UPPER_BOUND_REACHED, 0, CounterState.VALID);
      assertNextEvent(handle, 0, CounterState.VALID, 0, CounterState.LOWER_BOUND_REACHED);
      assertNoEvents(handle);
      handle.remove();
   }

   @Override
   public <L extends CounterListener> Handle<L> addListenerTo(StrongCounter counter, L logger) {
      return counter.addListener(logger);
   }

   @Override
   public StrongCounter defineAndCreateCounter(String counterName, long initialValue) {
      final CounterManager counterManager = counterManagerSupplier.get();
      assertTrue(
            counterManager.defineCounter(counterName, builder(UNBOUNDED_STRONG).initialValue(initialValue).build()));
      return counterManager.getStrongCounter(counterName);
   }

   @Override
   public void add(StrongCounter counter, long delta, long result) {
      if (delta == 1) {
         assertEquals(result, (long) awaitCounterOperation(counter.incrementAndGet()));
      } else if (delta == -1) {
         assertEquals(result, (long) awaitCounterOperation(counter.decrementAndGet()));
      } else {
         assertEquals(result, (long) awaitCounterOperation(counter.addAndGet(delta)));
      }
   }

   @Override
   void remove(StrongCounter counter) {
      awaitCounterOperation(counter.remove());
   }

   @Override
   void assertCounterValue(StrongCounter counter, long value) {
      assertEquals(value, (long) awaitCounterOperation(counter.getValue()));
   }

   @Override
   void reset(StrongCounter counter) {
      awaitCounterOperation(counter.reset());
   }

   @Override
   List<CounterConfiguration> configurationsToTest() {
      return asList(
            builder(UNBOUNDED_STRONG).initialValue(5).build(),
            builder(BOUNDED_STRONG).initialValue(0).lowerBound(-1).upperBound(1).build()
      );
   }

   @Override
   void assertCounterNameAndConfiguration(String counterName, CounterConfiguration configuration) {
      allCounterManagerSupplier.get().forEach(counterManager -> {
         StrongCounter counter = counterManager.getStrongCounter(counterName);
         assertEquals(counterName, counter.getName());
         assertEquals(configuration, counter.getConfiguration());
      });
   }

   private void assertNextEvent(Handle<EventLogger> handle, long oldValue, CounterState oldState, long newValue,
         CounterState newState)
         throws InterruptedException {
      CounterEvent event = handle.getCounterListener().waitingPoll();
      assertNotNull(event);
      assertEquals(oldValue, event.getOldValue());
      assertEquals(oldState, event.getOldState());
      assertEquals(newValue, event.getNewValue());
      assertEquals(newState, event.getNewState());
   }
}
