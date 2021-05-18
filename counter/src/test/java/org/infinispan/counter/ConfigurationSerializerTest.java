package org.infinispan.counter;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.JsonReader;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.CounterManagerConfiguration;
import org.infinispan.counter.configuration.CounterManagerConfigurationBuilder;
import org.infinispan.counter.configuration.Reliability;
import org.infinispan.counter.configuration.StrongCounterConfiguration;
import org.infinispan.counter.configuration.StrongCounterConfigurationBuilder;
import org.infinispan.counter.configuration.WeakCounterConfiguration;
import org.infinispan.counter.configuration.WeakCounterConfigurationBuilder;
import org.infinispan.counter.exception.CounterConfigurationException;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.DataProvider;
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

   @DataProvider
   public static Object[][] configurationFiles() {
      return new Object[][]{{Paths.get("config/counters.xml")}, {Paths.get("config/counters-9.4.xml")}};
   }

   public void testParser() throws IOException {
      ConfigurationBuilderHolder holder = new ParserRegistry().parseFile("config/counters.xml");

      GlobalConfiguration globalConfiguration = holder.getGlobalConfigurationBuilder().build();
      CounterManagerConfiguration counterManagerConfiguration = globalConfiguration
            .module(CounterManagerConfiguration.class);
      assertNotNull(counterManagerConfiguration);
      assertEquals(3, counterManagerConfiguration.numOwners());
      assertEquals(Reliability.CONSISTENT, counterManagerConfiguration.reliability());
      Map<String, AbstractCounterConfiguration> counterConfig = new HashMap<>();
      for (AbstractCounterConfiguration configuration : counterManagerConfiguration.counters()) {
         counterConfig.put(configuration.name(), configuration);
      }
      assertStrongCounter("c1", counterConfig.get("c1"), 1, Storage.PERSISTENT, false, Long.MIN_VALUE,
                          Long.MAX_VALUE);
      assertStrongCounter("c2", counterConfig.get("c2"), 2, Storage.VOLATILE, true, 0, Long.MAX_VALUE);
      assertStrongCounter("c3", counterConfig.get("c3"), 3, Storage.PERSISTENT, true, Long.MIN_VALUE, 5);
      assertStrongCounter("c4", counterConfig.get("c4"), 4, Storage.VOLATILE, true, 0, 10);
      assertWeakCounter(counterConfig.get("c5"));
   }

   public void testInvalid() throws IOException {
      Exceptions.expectException(CacheConfigurationException.class, CounterConfigurationException.class, () -> new ParserRegistry().parseFile("config/invalid.xml"));
   }

   @Test(dataProvider = "configurationFiles")
   public void jsonSerializationTest(Path config) throws Exception {
      JsonWriter jsonWriter = new JsonWriter();
      Properties properties = new Properties();
      properties.put("jboss.server.temp.dir", CommonsTestingUtil.tmpDirectory(ConfigurationSerializerTest.class));
      ParserRegistry registry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), false, properties);
      URL url = FileLookupFactory.newInstance().lookupFileLocation(config.toString(), Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holderBefore = registry.parse(url);
      CounterManagerConfigurationBuilder counterManagerConfigurationBuilder = (CounterManagerConfigurationBuilder) holderBefore.getGlobalConfigurationBuilder().modules().iterator().next();
      JsonReader jsonReader = new JsonReader();
      CounterManagerConfiguration confBefore = counterManagerConfigurationBuilder.create();
      List<AbstractCounterConfiguration> counters = confBefore.counters();
      for (AbstractCounterConfiguration beforeConf : counters) {
         String json = jsonWriter.toJSON(beforeConf);
         if (beforeConf instanceof StrongCounterConfiguration) {
            StrongCounterConfigurationBuilder builder = new StrongCounterConfigurationBuilder(counterManagerConfigurationBuilder);
            jsonReader.readJson(builder, json);
            StrongCounterConfiguration confAfter = builder.create();
            assertSameStrongCounterConfiguration(confAfter, beforeConf);
         }
         if (beforeConf instanceof WeakCounterConfiguration) {
            WeakCounterConfigurationBuilder builder = new WeakCounterConfigurationBuilder(counterManagerConfigurationBuilder);
            jsonReader.readJson(builder, json);
            WeakCounterConfiguration confAfter = builder.create();
            assertSameWeakCounterConfiguration(confAfter, beforeConf);
         }
      }
   }

   @Override
   protected void compareExtraGlobalConfiguration(GlobalConfiguration configurationBefore,
         GlobalConfiguration configurationAfter) {
      CounterManagerConfiguration configBefore = configurationBefore.module(CounterManagerConfiguration.class);
      CounterManagerConfiguration configAfter = configurationAfter.module(CounterManagerConfiguration.class);

      assertEquals(configBefore.numOwners(), configAfter.numOwners());
      assertEquals(configBefore.reliability(), configAfter.reliability());

      Map<String, AbstractCounterConfiguration> counterConfigBefore = new HashMap<>();
      for (AbstractCounterConfiguration configuration : configBefore.counters()) {
         counterConfigBefore.put(configuration.name(), configuration);
      }

      Map<String, AbstractCounterConfiguration> counterConfigAfter = new HashMap<>();
      for (AbstractCounterConfiguration configuration : configAfter.counters()) {
         counterConfigAfter.put(configuration.name(), configuration);
      }
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
