package org.infinispan.cdi.embedded.test.cachemanager.programmatic;

import static org.testng.Assert.assertEquals;

import org.infinispan.AdvancedCache;
import org.infinispan.cdi.embedded.test.testutil.Deployments;
import org.infinispan.commons.test.TestResourceTrackingListener;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import jakarta.inject.Inject;
import jakarta.inject.Named;


/**
 * Tests for a cache container defined programmatically.
 *
 * @author Pete Muir
 * @see Config
 */
@Listeners(TestResourceTrackingListener.class)
@Test(groups = {"functional", "smoke"}, testName = "cdi.test.cachemanager.embedded.programmatic.ProgrammaticCacheContainerTest")
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
   @Named("large")
   private AdvancedCache<?, ?> largeCache;

   @Inject
   @Named("super-large")
   private AdvancedCache<?, ?> superLargeCache;

   @Inject
   private SmallCacheObservers observers;

   public void testSmallCache() {
      assertEquals(smallCache.getCacheConfiguration().memory().maxCount(), 7);
      assertEquals(observers.getCacheStartedEventCount(), 1);
   }

   public void testLargeCache() {
      assertEquals(largeCache.getCacheConfiguration().memory().maxCount(), 10);
   }

   public void testSuperLargeCache() {
      assertEquals(superLargeCache.getCacheConfiguration().memory().maxCount(), 20);
   }
}
