package org.infinispan.counter.jmx;

import static org.infinispan.counter.api.CounterConfiguration.builder;
import static org.infinispan.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.infinispan.Cache;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.PropertyFormatter;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.counter.impl.BaseCounterTest;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.counter.util.Utils;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.test.TestingUtil;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * JMX operations tests.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@Test(groups = "functional", testName = "counter.jmx.CounterJmxTest")
public class CounterJmxTest extends BaseCounterTest {

   private static void assertCollection(Collection<String> expected, Collection<String> actual) {
      List<String> expectedList = new ArrayList<>(expected);
      Collections.sort(expectedList);
      List<String> actualList = new ArrayList<>(actual);
      Collections.sort(actualList);
      assertEquals(expectedList, actualList);
   }

   private static CounterConfiguration fromProperties(Properties properties) {
      return PropertyFormatter.getInstance().from(properties).build();
   }

   public void testDefinedCounters() throws Exception {
      final Collection<String> list = new ArrayList<>(3);
      assertJmxResult(list, this::executeCountersOperation, CounterJmxTest::assertCollection);
      addCounterAndCheckList(list, "A", builder(CounterType.WEAK).build());
      addCounterAndCheckList(list, "B", builder(CounterType.BOUNDED_STRONG).build());
      addCounterAndCheckList(list, "C", builder(CounterType.UNBOUNDED_STRONG).build());
   }

   public void testGetValueAndReset() throws Exception {
      checkValueAndReset("A", builder(CounterType.WEAK).initialValue(10).build(), 20,
            s -> addToWeakCounter(s, 10),
            this::resetWeakCounter);
      checkValueAndReset("B", builder(CounterType.UNBOUNDED_STRONG).initialValue(-10).build(), 5,
            s -> addToStrongCounter(s, 15, false),
            this::resetStrongCounter);
      checkValueAndReset("C", builder(CounterType.BOUNDED_STRONG).initialValue(1).lowerBound(0).upperBound(2).build(),
            2,
            s -> addToStrongCounter(s, 3, true),
            this::resetStrongCounter);
   }

   public void testRemove() {
      checkRemove("A", builder(CounterType.WEAK).initialValue(10).build(), 121, 20,
            s -> addToWeakCounter(s, 111),
            s -> addToWeakCounter(s, 10),
            this::getWeakCounterValue);

      checkRemove("B", builder(CounterType.UNBOUNDED_STRONG).initialValue(-10).build(), -11, -9,
            s -> addToStrongCounter(s, -1, false),
            s -> addToStrongCounter(s, 1, false),
            this::getStrongCounterValue);
   }

   public void testGetConfiguration() {
      assertConfiguration("A", createWeakCounterProperties(),
            builder(CounterType.WEAK).initialValue(10).concurrencyLevel(1).build());
      assertConfiguration("B", createUnboundedCounterProperties(),
            builder(CounterType.UNBOUNDED_STRONG).initialValue(5).build());
      assertConfiguration("C", createBoundedCounterProperties(),
            builder(CounterType.BOUNDED_STRONG).initialValue(5).lowerBound(0).upperBound(10).build());
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      super.clearContent();
      findCache(CounterModuleLifecycle.COUNTER_CACHE_NAME).ifPresent(Cache::clear);
      findCache(CounterModuleLifecycle.COUNTER_CONFIGURATION_CACHE_NAME).ifPresent(Cache::clear);
   }

   @Override
   protected int clusterSize() {
      return 2;
   }

   @Override
   protected GlobalConfigurationBuilder configure(int nodeId) {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      builder.globalJmxStatistics()
            .enable()
            .mBeanServerLookup(new PerThreadMBeanServerLookup());
      return builder;
   }

   private long getStrongCounterValue(String name) {
      return Utils.awaitCounterOperation(counterManager(0).getStrongCounter(name).getValue());
   }

   private long getWeakCounterValue(String name) {
      return counterManager(0).getWeakCounter(name).getValue();
   }

   private void assertConfiguration(String name, Properties props, CounterConfiguration config) {
      assertTrue(counterManager(0).defineCounter(name, config));
      assertJmxResult(props,
            i -> executeCounterNameArgOperation(i, "configuration", name),
            AssertJUnit::assertEquals);
      assertEquals(props, PropertyFormatter.getInstance().format(config));
      assertEquals(config, fromProperties(props));
   }

   private void resetStrongCounter(String name) {
      Utils.awaitCounterOperation(counterManager(0).getStrongCounter(name).reset());
   }

   private void addToStrongCounter(String name, long delta, boolean exception) {
      CompletableFuture<Long> result = counterManager(0).getStrongCounter(name).addAndGet(delta);
      if (exception) {
         expectException(CounterOutOfBoundsException.class, () -> Utils.awaitCounterOperation(result));
      } else {
         Utils.awaitCounterOperation(result);
      }
   }

   private void addToWeakCounter(String name, long delta) {
      Utils.awaitCounterOperation(counterManager(0).getWeakCounter(name).add(delta));
   }

   private void resetWeakCounter(String name) {
      Utils.awaitCounterOperation(counterManager(0).getWeakCounter(name).reset());
   }

   private void checkValueAndReset(String name, CounterConfiguration config, long addResult, Consumer<String> add,
         Consumer<String> reset) {
      assertTrue(counterManager(0).defineCounter(name, config));
      assertJmxResult(config.initialValue(), i -> executeCounterNameArgOperation(i, "value", name),
            AssertJUnit::assertEquals);
      add.accept(name);
      assertJmxResult(addResult, i -> executeCounterNameArgOperation(i, "value", name), AssertJUnit::assertEquals);
      reset.accept(name);
      assertJmxResult(config.initialValue(), i -> executeCounterNameArgOperation(i, "value", name),
            AssertJUnit::assertEquals);
   }

   private void checkRemove(String name, CounterConfiguration config, long add1Result, long add2Result,
         Consumer<String> add1, Consumer<String> add2, Function<String, Long> getValue) {
      assertTrue(counterManager(0).defineCounter(name, config));

      add1.accept(name);
      assertEquals(add1Result, (long) getValue.apply(name));
      executeCounterNameArgOperation(0, "remove", name);
      assertEquals(config.initialValue(), (long) getValue.apply(name));

      add2.accept(name);
      assertEquals(add2Result, (long) getValue.apply(name));
      executeCounterNameArgOperation(1, "remove", name);
      assertEquals(config.initialValue(), (long) getValue.apply(name));
   }

   private void addCounterAndCheckList(Collection<String> list, String name, CounterConfiguration config) {
      list.add(name);
      assertTrue(counterManager(0).defineCounter(name, config));
      assertJmxResult(list, this::executeCountersOperation, CounterJmxTest::assertCollection);
   }

   private <T> void assertJmxResult(T expected, IntFunction<T> function, BiConsumer<T, T> assertFunction) {
      for (int i = 0; i < clusterSize(); ++i) {
         assertFunction.accept(expected, function.apply(i));
      }
   }

   private Properties createWeakCounterProperties() {
      Properties properties = new Properties();
      properties.setProperty("type", "WEAK");
      properties.setProperty("storage", "VOLATILE");
      properties.setProperty("initial-value", "10");
      properties.setProperty("concurrency-level", "1");
      return properties;
   }

   private Properties createUnboundedCounterProperties() {
      Properties properties = new Properties();
      properties.setProperty("type", "UNBOUNDED_STRONG");
      properties.setProperty("storage", "VOLATILE");
      properties.setProperty("initial-value", "5");
      return properties;
   }

   private Properties createBoundedCounterProperties() {
      Properties properties = new Properties();
      properties.setProperty("type", "BOUNDED_STRONG");
      properties.setProperty("storage", "VOLATILE");
      properties.setProperty("initial-value", "5");
      properties.setProperty("lower-bound", "0");
      properties.setProperty("upper-bound", "10");
      return properties;
   }

   private Optional<Cache<?, ?>> findCache(String cacheName) {
      return Optional.ofNullable(manager(0).getCache(cacheName, false));
   }

   private Collection<String> executeCountersOperation(int index) {
      MBeanServer server = PerThreadMBeanServerLookup.getThreadMBeanServer();
      try {
         //noinspection unchecked
         return (Collection<String>) server.invoke(counterObjectName(index), "counters", new Object[0], new String[0]);
      } catch (InstanceNotFoundException | MBeanException | ReflectionException e) {
         throw new RuntimeException(e);
      }
   }

   private <T> T executeCounterNameArgOperation(int index, String operationName, String arg) {
      MBeanServer server = PerThreadMBeanServerLookup.getThreadMBeanServer();
      try {
         //noinspection unchecked
         return (T) server
               .invoke(counterObjectName(index), operationName, new Object[]{arg},
                     new String[]{String.class.getName()});
      } catch (InstanceNotFoundException | MBeanException | ReflectionException e) {
         throw new RuntimeException(e);
      }
   }

   private ObjectName counterObjectName(int managerIndex) {
      final String domain = manager(managerIndex).getCacheManagerConfiguration().globalJmxStatistics().domain();
      return TestingUtil.getCacheManagerObjectName(domain, "DefaultCacheManager", EmbeddedCounterManager.OBJECT_NAME);
   }
}
