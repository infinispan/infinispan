package org.infinispan.test.integration.embedded;

import static org.junit.Assert.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.junit.After;
import org.junit.Test;

/**
 * Test the Infinispan AS module integration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractInfinispanCoreIT {

   private EmbeddedCacheManager cm;

   @After
   public void cleanUp() {
      if (cm != null)
         cm.stop();
   }

   @Test
   public void testCacheManager() {
      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      holder.newConfigurationBuilder("cache");
      cm = new DefaultCacheManager(holder);
      Cache<String, String> cache = cm.getCache("cache");
      cache.put("a", "a");
      assertEquals("a", cache.get("a"));
   }
}
