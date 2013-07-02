package org.infinispan.config;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 4.2
 */
@Test (groups = "functional", testName = "config.ProgrammaticNameSetConfigTest", enabled = false,
       description = "Disabled as this functionality is not working for Old API and for New API it is useless.")
public class ProgrammaticNameSetConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(getDefaultStandaloneConfig(true));
   }

   public void testGetNotDefaultCache() {
      Configuration configurationOverride = new Configuration();
      configurationOverride.fluent().mode(Configuration.CacheMode.LOCAL);
      String aName = "aName";
      Configuration configuration = cacheManager.defineConfiguration(aName, configurationOverride);
      Cache c = cacheManager.getCache(aName);
      assertEquals(c.getConfiguration().getName(), aName);
      assertEquals(configuration.getName(), aName);
   }

   public void testGetNameForDefaultCache() {
      String name = cacheManager.getCache().getConfiguration().getName();
      assertEquals(name, CacheContainer.DEFAULT_CACHE_NAME);
   }

   public void getNameForUndefinedCache() {
      Configuration configuration = cacheManager.getCache("undefinedCache").getConfiguration();
      assertEquals(configuration.getName(), "undefinedCache");
   }
}
