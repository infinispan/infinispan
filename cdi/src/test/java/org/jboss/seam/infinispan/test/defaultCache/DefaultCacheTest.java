package org.jboss.seam.infinispan.test.defaultCache;

import static org.jboss.seam.infinispan.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.jboss.arquillian.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

/**
 * Tests that the default cache is available
 *
 * @author Pete Muir
 */
public class DefaultCacheTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addPackage(DefaultCacheTest.class.getPackage());
   }

   /**
    * The default cache can be injected with no configuration
    */
   @Inject
   private Cache<String, String> cache;

   @Inject
   private AdvancedCache<String, String> advancedCache;

   @Test(groups = "functional")
   public void testDefaultCache() {
      // Simple test to make sure the default cache works
      cache.put("pete", "British");
      cache.put("manik", "Sri Lankan");
      assertEquals(cache.get("pete"), "British");
      assertEquals(cache.get("manik"), "Sri Lankan");
      /*
       * Check that the advanced cache contains the same data as the simple
       * cache. As we can inject either Cache or AdvancedCache, this is double
       * checking that they both refer to the same underlying impl and Seam
       * Clouds isn't returning the wrong thing.
       */
      assertEquals(advancedCache.get("pete"), "British");
      assertEquals(advancedCache.get("manik"), "Sri Lankan");
   }

}
