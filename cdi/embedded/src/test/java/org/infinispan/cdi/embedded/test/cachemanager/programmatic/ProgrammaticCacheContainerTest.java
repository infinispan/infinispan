package org.infinispan.cdi.embedded.test.cachemanager.programmatic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.AdvancedCache;
import org.infinispan.cdi.embedded.test.testutil.Deployments;
import org.infinispan.testing.TestResourceTrackingListener;
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
      assertEquals(7, smallCache.getCacheConfiguration().memory().maxCount());
      assertEquals(1, observers.getCacheStartedEventCount());
   }

   public void testLargeCache() {
      assertEquals(10, largeCache.getCacheConfiguration().memory().maxCount());
   }

   public void testSuperLargeCache() {
      assertEquals(20, superLargeCache.getCacheConfiguration().memory().maxCount());
   }
}
