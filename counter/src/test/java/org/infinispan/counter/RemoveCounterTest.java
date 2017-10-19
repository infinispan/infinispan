package org.infinispan.counter;

import static org.infinispan.counter.api.CounterConfiguration.builder;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;

import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.impl.BaseCounterTest;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.counter.util.StrongTestCounter;
import org.infinispan.counter.util.TestCounter;
import org.infinispan.counter.util.WeakTestCounter;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@Test(groups = "functional", testName = "counter.RemoveCounterTest")
public class RemoveCounterTest extends BaseCounterTest {

   public void testCounterManagerRemoveWithUnbounded(Method method) {
      testCounterManagerRemove(Factory.UNBOUNDED, method.getName());
   }

   public void testCounterManagerRemoveWithBounded(Method method) {
      testCounterManagerRemove(Factory.BOUNDED, method.getName());
   }

   public void testCounterManagerRemoveWithWeak(Method method) {
      testCounterManagerRemove(Factory.WEAK, method.getName());
   }

   public void testCounterManagerRemoveNonExistingWithUnbounded(Method method) {
      testCounterManagerRemoveNonExisting(Factory.UNBOUNDED, method.getName());
   }

   public void testCounterManagerNonExistingRemoveWithBounded(Method method) {
      testCounterManagerRemoveNonExisting(Factory.BOUNDED, method.getName());
   }

   public void testCounterManagerNonExistingRemoveWithWeak(Method method) {
      testCounterManagerRemoveNonExisting(Factory.WEAK, method.getName());
   }

   public void testCounterRemoveWithUnbounded(Method method) {
      testCounterRemove(Factory.UNBOUNDED, method.getName());
   }

   public void testCounterRemoveWithBounded(Method method) {
      testCounterRemove(Factory.BOUNDED, method.getName());
   }

   public void testCounterRemoveWithWeak(Method method) {
      testCounterRemove(Factory.BOUNDED, method.getName());
   }

   @Override
   protected int clusterSize() {
      return 3;
   }

   private void testCounterRemove(Factory factory, String counterName) {
      CounterManager manager = counterManager(0);
      factory.define(manager, counterName);
      TestCounter counter = factory.get(manager, counterName);
      assertEquals(10, counter.getValue());
      assertTrue(counter.isSame(factory.get(manager, counterName)));

      assertCounterRemove(counterName, counter, factory);
      counter.increment();
      assertEquals(11, counter.getValue());

      assertCounterRemove(counterName, counter, factory);
      counter.decrement();
      assertEquals(9, counter.getValue());

      assertCounterRemove(counterName, counter, factory);
      counter.reset();
      assertEquals(10, counter.getValue());
   }

   private void testCounterManagerRemove(Factory factory, String counterName) {
      CounterManager manager = counterManager(0);
      factory.define(manager, counterName);
      TestCounter counter = factory.get(manager, counterName);
      assertEquals(10, counter.getValue());
      assertTrue(counter.isSame(factory.get(manager, counterName)));

      counter = assertCounterManagerRemove(counterName, counter, factory, 0);
      counter.increment();
      assertEquals(11, counter.getValue());

      counter = assertCounterManagerRemove(counterName, counter, factory, 0);
      counter.decrement();
      assertEquals(9, counter.getValue());

      counter = assertCounterManagerRemove(counterName, counter, factory, 0);
      counter.reset();
      assertEquals(10, counter.getValue());
   }

   private TestCounter assertCounterManagerRemove(String name, TestCounter counter, Factory factory, int index) {
      CounterManager manager = counterManager(index);
      manager.remove(name);
      assertTrue(cache(0, CounterModuleLifecycle.COUNTER_CACHE_NAME).isEmpty());
      TestCounter anotherCounter = factory.get(manager, name);
      if (counter != null) {
         assertFalse(counter.isSame(anotherCounter));
      }
      return anotherCounter;
   }

   private void assertCounterRemove(String name, TestCounter counter, Factory factory) {
      CounterManager manager = counterManager(0);
      counter.remove();
      assertTrue(cache(0, CounterModuleLifecycle.COUNTER_CACHE_NAME).isEmpty());
      TestCounter anotherCounter = factory.get(manager, name);
      assertTrue(counter.isSame(anotherCounter));
   }

   private void testCounterManagerRemoveNonExisting(Factory factory, String counterName) {
      //similar to testCounterManagerRemove but the remove() will be invoked in the CounterManager where the counter instance doesn't exist,
      CounterManager manager = counterManager(0);
      factory.define(manager, counterName);
      TestCounter counter = factory.get(manager, counterName);
      assertEquals(10, counter.getValue());
      assertTrue(counter.isSame(factory.get(manager, counterName)));

      counter = assertCounterManagerRemove(counterName, counter, factory, 1);
      counter.increment();
      assertEquals(11, counter.getValue());

      counter = assertCounterManagerRemove(counterName, counter, factory, 1);
      counter.decrement();
      assertEquals(9, counter.getValue());

      counter = assertCounterManagerRemove(counterName, counter, factory, 1);
      counter.reset();
      assertEquals(10, counter.getValue());
   }

   private enum Factory {
      UNBOUNDED {
         @Override
         void define(CounterManager manager, String name) {
            manager.defineCounter(name, builder(CounterType.UNBOUNDED_STRONG).initialValue(10).build());
         }

         @Override
         TestCounter get(CounterManager manager, String name) {
            return new StrongTestCounter(manager.getStrongCounter(name));
         }
      },
      BOUNDED {
         @Override
         void define(CounterManager manager, String name) {
            manager.defineCounter(name,
                  builder(CounterType.BOUNDED_STRONG).initialValue(10).lowerBound(0).upperBound(20).build());
         }

         @Override
         TestCounter get(CounterManager manager, String name) {
            return new StrongTestCounter(manager.getStrongCounter(name));
         }
      },
      WEAK {
         @Override
         void define(CounterManager manager, String name) {
            manager.defineCounter(name, builder(CounterType.WEAK).initialValue(10).build());
         }

         @Override
         TestCounter get(CounterManager manager, String name) {
            return new WeakTestCounter(manager.getWeakCounter(name));
         }
      };

      abstract void define(CounterManager manager, String name);

      abstract TestCounter get(CounterManager manager, String name);
   }
}
