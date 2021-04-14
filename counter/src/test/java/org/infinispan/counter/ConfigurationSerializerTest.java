package org.infinispan.counter;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.CounterManagerConfiguration;
import org.infinispan.counter.configuration.Reliability;
import org.infinispan.counter.configuration.StrongCounterConfiguration;
import org.infinispan.counter.configuration.WeakCounterConfiguration;
import org.infinispan.counter.exception.CounterConfigurationException;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * Tests the configuration parser and serializer.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "counter.ConfigurationSerializerTest")
@CleanupAfterMethod
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   public void testParser() throws IOException {
      ConfigurationBuilderHolder holder = new ParserRegistry().parseFile("configs/all/counters.xml");

      GlobalConfiguration globalConfiguration = holder.getGlobalConfigurationBuilder().build();
      CounterManagerConfiguration counterManagerConfiguration = globalConfiguration
            .module(CounterManagerConfiguration.class);
      assertNotNull(counterManagerConfiguration);
      assertEquals(3, counterManagerConfiguration.numOwners());
      assertEquals(Reliability.CONSISTENT, counterManagerConfiguration.reliability());
      Map<String, AbstractCounterConfiguration> counters = counterManagerConfiguration.counters();

      assertStrongCounter("c1", counters.get("c1"), 1, Storage.PERSISTENT, false, Long.MIN_VALUE,
                          Long.MAX_VALUE);
      assertStrongCounter("c2", counters.get("c2"), 2, Storage.VOLATILE, true, 0, Long.MAX_VALUE);
      assertStrongCounter("c3", counters.get("c3"), 3, Storage.PERSISTENT, true, Long.MIN_VALUE, 5);
      assertStrongCounter("c4", counters.get("c4"), 4, Storage.VOLATILE, true, 0, 10);
      assertWeakCounter(counters.get("c5"));
   }

   public void testInvalid() {
      Exceptions.expectException(CacheConfigurationException.class, CounterConfigurationException.class, () -> new ParserRegistry().parseFile("configs/invalid.xml"));
   }

   @Override
   protected void compareExtraGlobalConfiguration(GlobalConfiguration configurationBefore,
         GlobalConfiguration configurationAfter) {
      CounterManagerConfiguration configBefore = configurationBefore.module(CounterManagerConfiguration.class);
      CounterManagerConfiguration configAfter = configurationAfter.module(CounterManagerConfiguration.class);

      assertEquals(configBefore.numOwners(), configAfter.numOwners());
      assertEquals(configBefore.reliability(), configAfter.reliability());

      Map<String, AbstractCounterConfiguration> counterConfigBefore = configBefore.counters();
      Map<String, AbstractCounterConfiguration> counterConfigAfter = configAfter.counters();
      assertSameStrongCounterConfiguration(counterConfigBefore.get("c1"), counterConfigAfter.get("c1"));
      assertSameStrongCounterConfiguration(counterConfigBefore.get("c2"), counterConfigAfter.get("c2"));
      assertSameStrongCounterConfiguration(counterConfigBefore.get("c3"), counterConfigAfter.get("c3"));
      assertSameStrongCounterConfiguration(counterConfigBefore.get("c4"), counterConfigAfter.get("c4"));
      assertSameWeakCounterConfiguration(counterConfigBefore.get("c5"), counterConfigAfter.get("c5"));
   }

   private void assertSameStrongCounterConfiguration(AbstractCounterConfiguration c1, AbstractCounterConfiguration c2) {
      assertTrue(c1 instanceof StrongCounterConfiguration);
      assertTrue(c2 instanceof StrongCounterConfiguration);
      assertEquals(c1.name(), c2.name());
      assertEquals(c1.initialValue(), c2.initialValue());
      assertEquals(c1.storage(), c2.storage());
      assertEquals(((StrongCounterConfiguration) c1).isBound(), ((StrongCounterConfiguration) c2).isBound());
      assertEquals(((StrongCounterConfiguration) c1).lowerBound(), ((StrongCounterConfiguration) c2).lowerBound());
      assertEquals(((StrongCounterConfiguration) c1).upperBound(), ((StrongCounterConfiguration) c2).upperBound());
   }

   private void assertSameWeakCounterConfiguration(AbstractCounterConfiguration c1, AbstractCounterConfiguration c2) {
      assertTrue(c1 instanceof WeakCounterConfiguration);
      assertTrue(c2 instanceof WeakCounterConfiguration);
      assertEquals(c1.name(), c2.name());
      assertEquals(c1.initialValue(), c2.initialValue());
      assertEquals(c1.storage(), c2.storage());
      assertEquals(((WeakCounterConfiguration) c1).concurrencyLevel(),
            ((WeakCounterConfiguration) c2).concurrencyLevel());
   }

   private void assertWeakCounter(AbstractCounterConfiguration configuration) {
      assertTrue(configuration instanceof WeakCounterConfiguration);
      assertEquals("c5", configuration.name());
      assertEquals((long) 5, configuration.initialValue());
      assertEquals(Storage.PERSISTENT, configuration.storage());
      assertEquals(1, ((WeakCounterConfiguration) configuration).concurrencyLevel());
   }

   private void assertStrongCounter(String name, AbstractCounterConfiguration configuration, long initialValue,
         Storage storage, boolean bound, long lowerBound, long upperBound) {
      assertTrue(configuration instanceof StrongCounterConfiguration);
      assertEquals(name, configuration.name());
      assertEquals(initialValue, configuration.initialValue());
      assertEquals(storage, configuration.storage());
      assertEquals(bound, ((StrongCounterConfiguration) configuration).isBound());
      assertEquals(lowerBound, ((StrongCounterConfiguration) configuration).lowerBound());
      assertEquals(upperBound, ((StrongCounterConfiguration) configuration).upperBound());
   }

}
