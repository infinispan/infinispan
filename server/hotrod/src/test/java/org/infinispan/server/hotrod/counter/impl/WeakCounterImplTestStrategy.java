package org.infinispan.server.hotrod.counter.impl;

import static java.util.Collections.singletonList;
import static org.infinispan.counter.api.CounterConfiguration.builder;
import static org.infinispan.counter.api.CounterType.WEAK;
import static org.infinispan.counter.impl.Util.awaitCounterOperation;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.server.hotrod.counter.WeakCounterTestStrategy;

/**
 * The {@link WeakCounterTestStrategy} implementation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class WeakCounterImplTestStrategy extends BaseCounterImplTest<WeakCounter> implements WeakCounterTestStrategy {

   private final Supplier<Collection<CounterManager>> allCounterManagerSupplier;

   public WeakCounterImplTestStrategy(Supplier<CounterManager> counterManagerSupplier,
         Supplier<Collection<CounterManager>> allCounterManagerSupplier) {
      super(counterManagerSupplier);
      this.allCounterManagerSupplier = allCounterManagerSupplier;
   }

   @Override
   public <L extends CounterListener> Handle<L> addListenerTo(WeakCounter counter, L logger) {
      return counter.addListener(logger);
   }

   @Override
   public WeakCounter defineAndCreateCounter(String counterName, long initialValue) {
      final CounterManager counterManager = counterManagerSupplier.get();
      assertTrue(counterManager
            .defineCounter(counterName, builder(WEAK).initialValue(initialValue).concurrencyLevel(8).build()));
      return counterManager.getWeakCounter(counterName);
   }

   @Override
   public void add(WeakCounter counter, long delta, long result) {
      awaitCounterOperation(counter.add(delta));
   }

   @Override
   void remove(WeakCounter counter) {
      awaitCounterOperation(counter.remove());
   }

   @Override
   void assertCounterValue(WeakCounter counter, long value) {
      assertEquals(value, counter.getValue());
   }

   @Override
   void reset(WeakCounter counter) {
      awaitCounterOperation(counter.reset());
   }

   @Override
   List<CounterConfiguration> configurationsToTest() {
      return singletonList(builder(WEAK).initialValue(1).concurrencyLevel(2).build());
   }

   @Override
   void assertCounterNameAndConfiguration(String counterName, CounterConfiguration configuration) {
      allCounterManagerSupplier.get().forEach(counterManager -> {
         WeakCounter counter = counterManager.getWeakCounter(counterName);
         assertEquals(counterName, counter.getName());
         assertEquals(configuration, counter.getConfiguration());
      });
   }
}
