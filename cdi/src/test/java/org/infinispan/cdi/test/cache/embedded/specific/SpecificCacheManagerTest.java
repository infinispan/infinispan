package org.infinispan.cdi.test.cache.embedded.specific;

import org.infinispan.Cache;
import org.infinispan.cdi.InfinispanExtension;
import org.infinispan.cdi.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.infinispan.eviction.EvictionStrategy.LIRS;
import static org.infinispan.eviction.EvictionStrategy.NONE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
   private InfinispanExtension infinispanExtension;
   
   @Inject 
   private BeanManager beanManager;
   
   public void testCorrectCacheManagersRegistered() {
       assertEquals(infinispanExtension.getInstalledEmbeddedCacheManagers(beanManager).size(), 2);
   }

   public void testSpecificCacheManager() throws Exception {
      assertEquals(largeCache.getCacheConfiguration().eviction().maxEntries(), 2000);
      assertEquals(largeCache.getCacheConfiguration().eviction().strategy(), LIRS);
      assertEquals(largeCache.getCacheManager().getDefaultCacheConfiguration().eviction().maxEntries(), 4000);
      assertEquals(largeCache.getCacheManager().getDefaultCacheConfiguration().eviction().strategy(), LIRS);

      assertEquals(smallCache.getCacheConfiguration().eviction().maxEntries(), 20);
      assertEquals(smallCache.getCacheConfiguration().eviction().strategy(), LIRS);
      assertEquals(smallCache.getCacheManager().getDefaultCacheConfiguration().eviction().maxEntries(), 4000);
      assertEquals(smallCache.getCacheManager().getDefaultCacheConfiguration().eviction().strategy(), LIRS);

      // asserts that the small and large cache are defined in the same cache manager
      assertTrue(smallCache.getCacheManager().equals(largeCache.getCacheManager()));
      assertFalse(smallCache.getCacheManager().equals(cache.getCacheManager()));

      // check that the default configuration has not been modified
      assertEquals(cache.getCacheConfiguration().eviction().strategy(), NONE);
      assertEquals(cache.getCacheConfiguration().eviction().maxEntries(), -1);
      assertEquals(cache.getCacheManager().getDefaultCacheConfiguration().eviction().strategy(), NONE);
   }
}
