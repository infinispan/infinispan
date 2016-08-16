package org.infinispan.cdi.embedded.test.cache.specific;

import static org.infinispan.cdi.embedded.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;

import org.infinispan.Cache;
import org.infinispan.cdi.embedded.InfinispanExtensionEmbedded;
import org.infinispan.cdi.embedded.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.eviction.EvictionStrategy;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

/**
 * Tests that a specific cache manager can be used for one or more caches.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @see Config
 */
@Test(groups = "functional", testName = "cdi.test.cache.embedded.specific.SpecificCacheManagerTest")
public class SpecificCacheManagerTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
              .addPackage(SpecificCacheManagerTest.class.getPackage())
              .addClass(DefaultTestEmbeddedCacheManagerProducer.class);
   }

   @Inject
   private Cache<?, ?> cache;

   @Inject
   @Large
   private Cache<?, ?> largeCache;

   @Inject
   @Small
   private Cache<?, ?> smallCache;

   @Inject
   private InfinispanExtensionEmbedded infinispanExtension;

   @Inject
   private BeanManager beanManager;

   public void testCorrectCacheManagersRegistered() {
       assertEquals(infinispanExtension.getInstalledEmbeddedCacheManagers(beanManager).size(), 2);
   }

   public void testSpecificCacheManager() throws Exception {
      assertEquals(largeCache.getCacheConfiguration().eviction().maxEntries(), 2000);
      assertEquals(largeCache.getCacheConfiguration().eviction().strategy(), EvictionStrategy.LIRS);
      assertEquals(largeCache.getCacheManager().getDefaultCacheConfiguration().eviction().maxEntries(), 4000);
      assertEquals(largeCache.getCacheManager().getDefaultCacheConfiguration().eviction().strategy(), EvictionStrategy.LIRS);

      assertEquals(smallCache.getCacheConfiguration().eviction().maxEntries(), 20);
      assertEquals(smallCache.getCacheConfiguration().eviction().strategy(), EvictionStrategy.LIRS);
      assertEquals(smallCache.getCacheManager().getDefaultCacheConfiguration().eviction().maxEntries(), 4000);
      assertEquals(smallCache.getCacheManager().getDefaultCacheConfiguration().eviction().strategy(), EvictionStrategy.LIRS);

      // asserts that the small and large cache are defined in the same cache manager
      assertTrue(smallCache.getCacheManager().equals(largeCache.getCacheManager()));
      assertFalse(smallCache.getCacheManager().equals(cache.getCacheManager()));

      // check that the default configuration has not been modified
      assertEquals(cache.getCacheConfiguration().eviction().strategy(), EvictionStrategy.NONE);
      assertEquals(cache.getCacheConfiguration().eviction().maxEntries(), -1);
      assertEquals(cache.getCacheManager().getDefaultCacheConfiguration().eviction().strategy(), EvictionStrategy.NONE);
   }
}
