package org.infinispan.test.integration.cdi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.cache.annotation.CacheKey;
import javax.inject.Inject;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Kevin Pollet &lt;pollet.kevin@gmail.com&gt; (C) 2011
 */
public abstract class AbstractGreetingServiceIT {

   @Inject
   @GreetingCache
   private org.infinispan.Cache<CacheKey, String> greetingCache;

   @Inject
   private GreetingService greetingService;

   @Before
   public void init() {
      greetingCache.clear();
      assertEquals(0, greetingCache.size());
   }

   @Test
   public void testGreetMethod() {
      assertEquals("Hello Pete :)", greetingService.greet("Pete"));
   }

   @Test
   public void testGreetMethodCache() {
      greetingService.greet("Pete");

      assertEquals(1, greetingCache.size());
      assertTrue(greetingCache.values().contains("Hello Pete :)"));

      greetingService.greet("Manik");

      assertEquals(2, greetingCache.size());
      assertTrue(greetingCache.values().contains("Hello Manik :)"));

      greetingService.greet("Pete");

      assertEquals(2, greetingCache.size());
   }
}
