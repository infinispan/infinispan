package org.infinispan.cdi.test.cachemanager.embedded.external;

import org.infinispan.AdvancedCache;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;


/**
 * Tests for a cache container defined by some external mechanism.
 *
 * @author Pete Muir
 * @see Config
 */
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
      assertEquals(largeCache.getCacheConfiguration().eviction().maxEntries(), 100);
   }

   public void testQuickCache() {
      assertEquals(quickCache.getCacheConfiguration().expiration().wakeUpInterval(), 1);
   }
}
