package org.infinispan.lock;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.infinispan.lock.configuration.ClusteredLockConfiguration;
import org.infinispan.lock.configuration.ClusteredLockManagerConfiguration;
import org.infinispan.lock.configuration.Reliability;
import org.infinispan.lock.impl.ClusteredLockModuleLifecycle;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests the configuration parser and serializer.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.4
 */
@Test(groups = "functional", testName = "counter.ConfigurationSerializerTest")
@CleanupAfterMethod
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @DataProvider
   public static Object[][] configurationFiles() {
      return new Object[][]{{Paths.get("config/clustered-locks-dist.xml")}, {Paths.get("config/clustered-locks-repl.xml")}};
   }

   public void testParserAvailableReliability() throws IOException {
      ConfigurationBuilderHolder holder = new ParserRegistry().parseFile("config/clustered-locks-dist.xml");
      withCacheManager(() -> createClusteredCacheManager(holder), cacheManager -> {
         cacheManager.getCache(ClusteredLockModuleLifecycle.CLUSTERED_LOCK_CACHE_NAME);
         GlobalConfiguration globalConfiguration = cacheManager.getGlobalComponentRegistry().getGlobalConfiguration();
         ClusteredLockManagerConfiguration clmConfig = globalConfiguration
               .module(ClusteredLockManagerConfiguration.class);
         assertNotNull(clmConfig);
         assertEquals(3, clmConfig.numOwners());
         assertEquals(Reliability.AVAILABLE, clmConfig.reliability());
         assertTrue(clmConfig.locks().containsKey("lock1"));
         assertTrue(clmConfig.locks().containsKey("lock2"));
      });
   }

   public void testParserConsistentReliability() throws IOException {
      ConfigurationBuilderHolder holder = new ParserRegistry().parseFile("config/clustered-locks-repl.xml");
      withCacheManager(() -> createClusteredCacheManager(holder), cacheManager -> {
         cacheManager.getCache(ClusteredLockModuleLifecycle.CLUSTERED_LOCK_CACHE_NAME);
         GlobalConfiguration globalConfiguration = cacheManager.getGlobalComponentRegistry().getGlobalConfiguration();
         ClusteredLockManagerConfiguration clmConfig = globalConfiguration
               .module(ClusteredLockManagerConfiguration.class);
         assertNotNull(clmConfig);
         assertEquals(-1, clmConfig.numOwners());
         assertEquals(Reliability.CONSISTENT, clmConfig.reliability());
         Map<String, ClusteredLockConfiguration> clusteredLockConfig = new HashMap<>();
         assertTrue(clmConfig.locks().containsKey("consi-lock1"));
         assertTrue(clmConfig.locks().containsKey("consi-lock2"));
      });
   }
}
