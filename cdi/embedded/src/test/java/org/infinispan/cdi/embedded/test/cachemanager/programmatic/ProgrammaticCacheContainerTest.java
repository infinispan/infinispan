package org.infinispan.cdi.embedded.test.cachemanager.programmatic;

import org.infinispan.AdvancedCache;
import org.infinispan.cdi.embedded.test.testutil.Deployments;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.testng.Assert.assertEquals;


/**
 * Tests for a cache container defined programmatically.
 *
 * @author Pete Muir
 * @see Config
 */
@Test(groups = "functional", testName = "cdi.test.cachemanager.embedded.programmatic.ProgrammaticCacheContainerTest")
public class ProgrammaticCacheContainerTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return Deployments.baseDeployment()
            .addPackage(ProgrammaticCacheContainerTest.class.getPackage());
   }

   @Inject
   @Small
   private AdvancedCache<?, ?> smallCache;

   @Inject
   private SmallCacheObservers observers;

   public void testSmallCache() {
      assertEquals(smallCache.getCacheConfiguration().eviction().maxEntries(), 7);
      assertEquals(observers.getCacheStartedEventCount(), 1);
   }
}
