package org.jboss.seam.infinispan.test.cacheManager.external;

import static org.jboss.seam.infinispan.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.infinispan.AdvancedCache;
import org.jboss.arquillian.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;


/**
 * Tests for a cache container defined by some external mechanism
 * 
 * @author Pete Muir
 * @see Config
 */
public class ExternalCacheContainerTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment().addPackage(
            ExternalCacheContainerTest.class.getPackage());
   }

   @Inject
   @Large
   private AdvancedCache<?, ?> largeCache;

   @Inject
   @Quick
   private AdvancedCache<?, ?> quickCache;

   @Test(groups = "functional")
   public void testLargeCache() {
      assertEquals(largeCache.getConfiguration().getEvictionMaxEntries(), 100);
   }

   @Test(groups = "functional")
   public void testQuickCache() {
      assertEquals(quickCache.getConfiguration().getEvictionWakeUpInterval(), 1);
   }

}
