package org.infinispan.multimap.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(groups = "functional", testName = "multimap.configuration.ConfigurationParserTest")
public class ConfigurationParserTest extends AbstractConfigurationSerializerTest {

   public void testParser() throws Exception {
      ConfigurationBuilderHolder holder = new ParserRegistry().parseFile("configs/all/multimap.xml");

      GlobalConfiguration globalConfiguration = holder.getGlobalConfigurationBuilder().build();
      MultimapCacheManagerConfiguration configuration = globalConfiguration.module(MultimapCacheManagerConfiguration.class);

      assertNotNull(configuration);
      assertEquals(3, configuration.multimaps().size());

      Map<String, EmbeddedMultimapConfiguration> configurations = configuration.multimaps();
      assertMultimapConfiguration(configurations.get("m1"), "m1", true);
      assertMultimapConfiguration(configurations.get("m2"), "m2", false);
      assertMultimapConfiguration(configurations.get("m3"), "m3", false);
   }

   public void testInvalidConfiguration() {
      Exceptions.expectException(CacheConfigurationException.class, "ISPN000542: Invalid parser scope. Expected 'CACHE_CONTAINER' but was 'CACHE'",
            () -> new ParserRegistry().parseFile("invalid.xml"));
   }

   static void assertMultimapConfiguration(EmbeddedMultimapConfiguration configuration, String name, boolean supportDuplicates) {
      assertNotNull(configuration);
      assertEquals(name, configuration.name());
      assertEquals(supportDuplicates, configuration.supportsDuplicates());
   }
}
