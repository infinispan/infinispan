package org.infinispan.cloudevents;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.infinispan.cloudevents.configuration.CloudEventsConfiguration;
import org.infinispan.cloudevents.configuration.CloudEventsGlobalConfiguration;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests the configuration parser and serializer.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "cloudevents.CloudEventsConfigurationSerializerTest")
@CleanupAfterMethod
public class CloudEventsConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @DataProvider
   public static Object[][] configurationFiles() {
      return new Object[][]{{Paths.get("config/cloudevents.xml")}};
   }

   public void testParser() throws IOException {
      ConfigurationBuilderHolder holder = new ParserRegistry().parseFile("config/cloudevents.xml");
      try (EmbeddedCacheManager cacheManager = createClusteredCacheManager(false, holder)) {
         CloudEventsGlobalConfiguration cloudEventsGlobalConfiguration =
               cacheManager.getCacheManagerConfiguration().module(CloudEventsGlobalConfiguration.class);

         assertEquals("127.0.0.1:9092", cloudEventsGlobalConfiguration.bootstrapServers());
         assertEquals("0", cloudEventsGlobalConfiguration.acks());
         assertEquals("audit", cloudEventsGlobalConfiguration.auditTopic());
         assertTrue(cloudEventsGlobalConfiguration.auditEventsEnabled());
         assertEquals("cache-events", cloudEventsGlobalConfiguration.cacheEntriesTopic());
         assertTrue(cloudEventsGlobalConfiguration.cacheEntryEventsEnabled());


         // Cache cache1 has cache entry events enabled
         Configuration cache1 = cacheManager.getCacheConfiguration("cache1");
         assertNull(cache1.module(CloudEventsConfiguration.class));

         // Cache cache2 has cache entry events disabled
         Configuration cache2 = cacheManager.getCacheConfiguration("cache2");
         assertFalse(cache2.module(CloudEventsConfiguration.class).enabled());
      }
   }

   public void testInvalid() throws IOException {
      ConfigurationBuilderHolder holder =
            new ParserRegistry().parseFile("config/cloudevents-missing-bootstrap-servers.xml");
      expectException(CacheConfigurationException.class, "ISPN030502: .*",
                      () -> holder.getGlobalConfigurationBuilder().build());
   }

   @Test(dataProvider = "configurationFiles")
   @Override
   public void jsonSerializationTest(Path config) throws Exception {
      // FIXME JSON deserialization doesn't handle custom configuration modules
      throw new SkipException("JSON deserialization doesn't work");
   }

   @Override
   protected void compareExtraGlobalConfiguration(GlobalConfiguration configurationBefore,
                                                  GlobalConfiguration configurationAfter) {
      CloudEventsGlobalConfiguration before = configurationBefore.module(CloudEventsGlobalConfiguration.class);
      CloudEventsGlobalConfiguration after = configurationAfter.module(CloudEventsGlobalConfiguration.class);
      assertNotNull(before);
      assertNotNull(after);
      assertEquals(before.bootstrapServers(), after.bootstrapServers());
      assertEquals(before.auditTopic(), after.auditTopic());
      assertEquals(before.cacheEntriesTopic(), after.cacheEntriesTopic());
   }

   @Override
   protected void compareExtraConfiguration(String name, Configuration configurationBefore,
                                            Configuration configurationAfter) {
      CloudEventsConfiguration before = configurationBefore.module(CloudEventsConfiguration.class);
      CloudEventsConfiguration after = configurationAfter.module(CloudEventsConfiguration.class);
      if (before == null && after == null)
         return;

      if (before == null || after == null) {
         // Only one of them is null
         fail("before=" + before + ", after=" + after);
      }

      assertEquals(before.enabled(), after.enabled());
   }
}
