package org.infinispan.test.integration.as.cdi;

import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.test.integration.as.category.UnstableTest;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.inject.Inject;

import static org.junit.Assert.*;

/**
 * @author Kevin Pollet <pollet.kevin@gmail.com> (C) 2011
 */
@RunWith(Arquillian.class)
@Category(UnstableTest.class)
public class GreetingCacheManagerIT {

   @Deployment
   public static WebArchive deployment() {
      return Deployments.baseDeployment()
            .addClass(GreetingCacheManagerIT.class);
   }

   @Inject
   private GreetingService greetingService;

   @Inject
   private GreetingCacheManager greetingCacheManager;

   @Test
   public void testGreetingCacheConfiguration() {
      // Cache name
      assertEquals("greeting-cache", greetingCacheManager.getCacheName());

      // Eviction
      assertEquals(4, greetingCacheManager.getEvictionMaxEntries());
      assertEquals(EvictionStrategy.LRU, greetingCacheManager.getEvictionStrategy());

      // Lifespan
      assertEquals(-1, greetingCacheManager.getExpirationLifespan());
   }

   @Test
   public void testGreetingCacheCachedValues() {
      greetingService.greet("Pete");

      assertEquals(1, greetingCacheManager.getCachedValues().length);
      assertEquals("Hello Pete :)", greetingCacheManager.getCachedValues()[0]);
   }

   @Test
   public void testClearGreetingCache() {
      greetingService.greet("Pete");

      assertEquals(1, greetingCacheManager.getNumberOfEntries());

      greetingCacheManager.clearCache();

      assertEquals(0, greetingCacheManager.getNumberOfEntries());
   }
}
