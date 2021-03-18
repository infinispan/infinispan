package org.infinispan.anchored;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.infinispan.Cache;
import org.infinispan.anchored.configuration.AnchoredKeysConfiguration;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.infinispan.counter.exception.CounterConfigurationException;
import org.infinispan.test.AbstractInfinispanTest;
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
@Test(groups = "functional", testName = "anchored.AnchoredConfigurationSerializerTest")
@CleanupAfterMethod
@AbstractInfinispanTest.FeatureCondition(feature = "anchored-keys")
public class AnchoredConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @DataProvider
   public static Object[][] configurationFiles() {
      return new Object[][]{{Paths.get("config/anchored.xml")}};
   }

   public void testParser() throws IOException {
      ConfigurationBuilderHolder holder = new ParserRegistry().parseFile("config/anchored.xml");
      withCacheManager(() -> createClusteredCacheManager(holder), cacheManager -> {
         Cache<Object, Object> anchoredCache = cacheManager.getCache();
         AnchoredKeysConfiguration anchoredKeysConfiguration =
               anchoredCache.getCacheConfiguration().module(AnchoredKeysConfiguration.class);
         assertTrue(anchoredKeysConfiguration.enabled());
      });
   }

   public void testInvalid() throws IOException {
      try {
         ConfigurationBuilderHolder holder = new ParserRegistry().parseFile("config/invalid.xml");
         fail("Expected exception. " + holder);
      } catch (CacheConfigurationException | CounterConfigurationException e) {
         log.debug("Expected exception", e);
      }
   }

   @Test(dataProvider = "configurationFiles")
   @Override
   public void jsonSerializationTest(Path config) throws Exception {
      // FIXME JSON deserialization doesn't handle custom configuration modules
      throw new SkipException("JSON deserialization doesn't work");
   }

   @Override
   protected void compareExtraConfiguration(String name, Configuration configurationBefore,
                                            Configuration configurationAfter) {
      AnchoredKeysConfiguration module = configurationAfter.module(AnchoredKeysConfiguration.class);
      assertNotNull(module);
      assertTrue(module.enabled());
   }
}
