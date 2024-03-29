package org.infinispan.cdi.embedded.test.cachemanager.registration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Set;

import jakarta.inject.Inject;

import org.infinispan.Cache;
import org.infinispan.cdi.embedded.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.cdi.embedded.test.testutil.Deployments;
import org.infinispan.commons.test.TestResourceTrackingListener;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

/**
 * Tests that configured caches are registered in the corresponding cache manager.
 *
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
@Listeners(TestResourceTrackingListener.class)
@Test(groups = "functional", testName = "cdi.test.cachemanager.embedded.registration.CacheRegistrationTest")
public class CacheRegistrationTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return Deployments.baseDeployment()
            .addPackage(CacheRegistrationTest.class.getPackage())
            .addClass(DefaultTestEmbeddedCacheManagerProducer.class);
   }

   @Inject
   private EmbeddedCacheManager defaultCacheManager;

   @Inject
   private Cache<String, String> cache;

   @VeryLarge
   @Inject
   private EmbeddedCacheManager specificCacheManager;

   public void testCacheRegistrationInDefaultCacheManager() {
       // Make sure the cache is registered
      cache.put("foo", "bar");

      final Set<String> cacheNames = defaultCacheManager.getCacheNames();

      assertEquals(cacheNames.size(), 3);
      assertTrue(cacheNames.containsAll(Arrays.asList("small", "large", TestCacheManagerFactory.DEFAULT_CACHE_NAME)));
   }

   public void testCacheRegistrationInSpecificCacheManager() {
      // Make sure the cache is registered
      cache.put("foo", "bar");
      final Set<String> cacheNames = specificCacheManager.getCacheConfigurationNames();

      assertEquals(cacheNames.size(), 1);
      assertTrue(cacheNames.containsAll(Arrays.asList("very-large")));
   }
}
