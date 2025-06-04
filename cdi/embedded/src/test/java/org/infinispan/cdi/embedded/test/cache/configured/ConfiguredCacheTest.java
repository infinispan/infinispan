package org.infinispan.cdi.embedded.test.cache.configured;

import static org.testng.Assert.assertEquals;

import org.infinispan.AdvancedCache;
import org.infinispan.cdi.embedded.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.cdi.embedded.test.testutil.Deployments;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import jakarta.inject.Inject;

/**
 * Tests that the simple form of configuration works.
 *
 * @author Pete Muir
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 * @see Config
 */
@Test(groups = {"functional", "smoke"}, testName = "cdi.test.cache.embedded.configured.ConfiguredCacheTest")
public class ConfiguredCacheTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return Deployments.baseDeployment()
            .addPackage(ConfiguredCacheTest.class.getPackage())
            .addClass(DefaultTestEmbeddedCacheManagerProducer.class);
   }

   @Inject
   @Tiny
   private AdvancedCache<?, ?> tinyCache;

   @Inject
   @Small
   private AdvancedCache<?, ?> smallCache;

   public void testTinyCache() {
      // Check that we have the correctly configured cache
      assertEquals(tinyCache.getCacheConfiguration().memory().maxCount(), 1);
   }

   public void testSmallCache() {
      // Check that we have the correctly configured cache
      assertEquals(smallCache.getCacheConfiguration().memory().maxCount(), 10);
   }
}
