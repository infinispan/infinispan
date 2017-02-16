package org.infinispan.cdi.embedded.test.cachemanager.xml;

import static org.infinispan.cdi.embedded.test.testutil.Deployments.baseDeployment;
import static org.testng.AssertJUnit.assertEquals;

import javax.inject.Inject;

import org.infinispan.AdvancedCache;
import org.infinispan.test.fwk.TestResourceTrackingListener;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

/**
 * Test that a cache configured in XML is available, and that it can be overridden.
 *
 * @author Pete Muir
 * @see Config
 */
@Listeners(TestResourceTrackingListener.class)
@Test(groups = "functional", testName = "cdi.test.cachemanager.embedded.xml.XMLConfiguredCacheContainerTest")
public class XMLConfiguredCacheContainerTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addPackage(XMLConfiguredCacheContainerTest.class.getPackage());
   }

   @Inject
   @VeryLarge
   private AdvancedCache<?, ?> largeCache;

   @Inject
   @Quick
   private AdvancedCache<?, ?> quickCache;

   public void testVeryLargeCache() {
      assertEquals(largeCache.getCacheConfiguration().memory().size(), 1000);
   }

   public void testQuickCache() {
      assertEquals(quickCache.getCacheConfiguration().memory().size(), 1000);
      assertEquals(quickCache.getCacheConfiguration().expiration().wakeUpInterval(), 1);
   }
}
