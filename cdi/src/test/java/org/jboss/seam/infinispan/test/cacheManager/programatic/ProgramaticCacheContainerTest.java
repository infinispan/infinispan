package org.jboss.seam.infinispan.test.cacheManager.programatic;

import static org.jboss.seam.infinispan.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.infinispan.AdvancedCache;
import org.jboss.arquillian.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;


/**
 * Tests for a cache container defined programatically
 * 
 * @author Pete Muir
 * @see Config
 */
public class ProgramaticCacheContainerTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment().addPackage(
            ProgramaticCacheContainerTest.class.getPackage());
   }

   @Inject
   @Small
   private AdvancedCache<?, ?> smallCache;

   @Inject
   SmallCacheObservers observers;

   @Test
   public void testSmallCache() {
      assertEquals(smallCache.getConfiguration().getEvictionMaxEntries(), 7);
      assertEquals(observers.getCacheStartedEventCount(), 1);
   }

}
