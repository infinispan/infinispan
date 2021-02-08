package org.infinispan.test.integration.cdi;

import static org.junit.Assert.assertEquals;

import javax.inject.Inject;

import org.infinispan.eviction.EvictionType;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Kevin Pollet &lt;pollet.kevin@gmail.com&gt; (C) 2011
 */
public abstract class AbstractGreetingCacheManagerIT {

   @Inject
   private GreetingService greetingService;

   @Inject
   private GreetingCacheManager greetingCacheManager;

   @Before
   public void init() {
      greetingCacheManager.clearCache();
      assertEquals(0, greetingCacheManager.getNumberOfEntries());
   }

   @Test
   public void testGreetingCacheConfiguration() {
      // Cache name
      assertEquals("greeting-cache", greetingCacheManager.getCacheName());

      // Eviction
      assertEquals(128, greetingCacheManager.getMemorySize());
      assertEquals(EvictionType.COUNT, greetingCacheManager.getEvictionType());

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
