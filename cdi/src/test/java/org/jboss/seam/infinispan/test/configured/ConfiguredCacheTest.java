package org.jboss.seam.infinispan.test.configured;

import static org.jboss.seam.infinispan.test.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.infinispan.AdvancedCache;
import org.jboss.arquillian.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

/**
 * Tests that the simple form of configuration works
 * 
 * @author Pete Muir
 * @see Config
 * 
 */
public class ConfiguredCacheTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addPackage(ConfiguredCacheTest.class.getPackage());
   }

   /**
    * Inject a cache configured by the application
    */
   @Inject
   @Tiny
   private AdvancedCache<String, String> tinyCache;

   /**
    * Inject a cache configured by application
    */
   @Inject
   @Small
   private AdvancedCache<String, String> smallCache;

   @Test(groups = "functional")
   public void testTinyCache() {
      // Check that we have the correctly configured cache
      assertEquals(tinyCache.getConfiguration().getEvictionMaxEntries(), 1);
   }

   @Test(groups = "functional")
   public void testSmallCache() {
      // Check that we have the correctly configured cache
      assertEquals(smallCache.getConfiguration().getEvictionMaxEntries(), 10);
   }

}
