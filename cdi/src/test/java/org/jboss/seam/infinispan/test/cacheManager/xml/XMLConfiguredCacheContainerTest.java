package org.jboss.seam.infinispan.test.cacheManager.xml;

import static org.jboss.seam.infinispan.test.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.infinispan.AdvancedCache;
import org.jboss.arquillian.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

/**
 * Test that a cache configured in XML is available, and that it can be
 * overridden
 * 
 * @see Config
 * @author Pete Muir
 * 
 */
public class XMLConfiguredCacheContainerTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment().addPackage(
            XMLConfiguredCacheContainerTest.class.getPackage());
   }

   @Inject
   @VeryLarge
   private AdvancedCache<?, ?> largeCache;

   @Inject
   @Quick
   private AdvancedCache<?, ?> quickCache;

   @Test(groups = "functional")
   public void testVeryLargeCache() {
      assertEquals(largeCache.getConfiguration().getEvictionMaxEntries(), 1000);
   }

   @Test(groups = "functional")
   public void testQuickCache() {
      assertEquals(quickCache.getConfiguration().getEvictionMaxEntries(), 1000);
      assertEquals(quickCache.getConfiguration().getEvictionWakeUpInterval(), 1);
   }

}
