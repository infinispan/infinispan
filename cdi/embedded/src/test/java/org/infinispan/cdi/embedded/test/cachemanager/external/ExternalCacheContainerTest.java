package org.infinispan.cdi.embedded.test.cachemanager.external;

import static org.infinispan.cdi.embedded.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.test.TestResourceTrackingListener;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import jakarta.inject.Inject;


/**
 * Tests for a cache container defined by some external mechanism.
 *
 * @author Pete Muir
 * @see Config
 */
@Listeners(TestResourceTrackingListener.class)
@Test(groups = "functional", testName = "cdi.test.cachemanager.embedded.external.ExternalCacheContainerTest")
public class ExternalCacheContainerTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addPackage(ExternalCacheContainerTest.class.getPackage());
   }

   @Inject
   @Large
   private AdvancedCache<?, ?> largeCache;

   @Inject
   @Quick
   private AdvancedCache<?, ?> quickCache;

   public void testLargeCache() {
      assertEquals(largeCache.getCacheConfiguration().memory().maxCount(), 100);
   }

   public void testQuickCache() {
      assertEquals(quickCache.getCacheConfiguration().expiration().wakeUpInterval(), 1);
   }
}
