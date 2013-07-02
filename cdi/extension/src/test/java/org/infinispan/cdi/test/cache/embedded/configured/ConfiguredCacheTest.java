package org.infinispan.cdi.test.cache.embedded.configured;

import org.infinispan.AdvancedCache;
import org.infinispan.cdi.test.DefaultTestEmbeddedCacheManagerProducer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;

/**
 * Tests that the simple form of configuration works.
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @see Config
 */
@Test(groups = "functional", testName = "cdi.test.cache.embedded.configured.ConfiguredCacheTest")
public class ConfiguredCacheTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
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
      assertEquals(tinyCache.getCacheConfiguration().eviction().maxEntries(), 1);
   }

   public void testSmallCache() {
      // Check that we have the correctly configured cache
      assertEquals(smallCache.getCacheConfiguration().eviction().maxEntries(), 10);
   }
}
