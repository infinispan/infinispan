package org.infinispan.cdi.test.cachemanager.embedded.xml;

import org.infinispan.AdvancedCache;
import org.infinispan.cdi.test.testutil.Deployments;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.testng.Assert.assertEquals;

/**
 * Test that a cache configured in XML is available, and that it can be overridden.
 *
 * @author Pete Muir
 * @see Config
 */
@Test(groups = "functional", testName = "cdi.test.cachemanager.embedded.xml.XMLConfiguredCacheContainerTest")
public class XMLConfiguredCacheContainerTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return Deployments.baseDeployment()
            .addPackage(XMLConfiguredCacheContainerTest.class.getPackage());
   }

   @Inject
   @VeryLarge
   private AdvancedCache<?, ?> largeCache;

   @Inject
   @Quick
   private AdvancedCache<?, ?> quickCache;

   public void testVeryLargeCache() {
      assertEquals(largeCache.getCacheConfiguration().eviction().maxEntries(), 1000);
   }

   public void testQuickCache() {
      assertEquals(quickCache.getCacheConfiguration().eviction().maxEntries(), 1000);
      assertEquals(quickCache.getCacheConfiguration().expiration().wakeUpInterval(), 1);
   }
}
