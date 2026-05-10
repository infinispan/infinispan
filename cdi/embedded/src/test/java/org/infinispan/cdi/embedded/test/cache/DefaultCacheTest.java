package org.infinispan.cdi.embedded.test.cache;

import static org.infinispan.cdi.embedded.test.testutil.Deployments.baseDeployment;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cdi.embedded.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import jakarta.inject.Inject;

/**
 * Tests that the default cache is available and can be injected with no configuration.
 *
 * @author Pete Muir
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
@Test(groups = {"functional", "smoke"}, testName = "cdi.test.cache.embedded.DefaultCacheTest")
public class DefaultCacheTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(DefaultCacheTest.class)
            .addClass(DefaultTestEmbeddedCacheManagerProducer.class);
   }

   @Inject
   private Cache<String, String> cache;

   @Inject
   private AdvancedCache<String, String> advancedCache;

   public void testDefaultCache() {
      // Simple test to make sure the default cache works
      cache.put("pete", "British");
      cache.put("manik", "Sri Lankan");
      assertEquals("British", cache.get("pete"));
      assertEquals("Sri Lankan", cache.get("manik"));
      assertEquals(TestCacheManagerFactory.DEFAULT_CACHE_NAME, cache.getName());
      /*
       * Check that the advanced cache contains the same data as the simple
       * cache. As we can inject either Cache or AdvancedCache, this is double
       * checking that they both refer to the same underlying impl and Seam
       * Clouds isn't returning the wrong thing.
       */
      assertEquals("British", advancedCache.get("pete"));
      assertEquals("Sri Lankan", advancedCache.get("manik"));
      assertEquals(TestCacheManagerFactory.DEFAULT_CACHE_NAME, advancedCache.getName());
   }
}
