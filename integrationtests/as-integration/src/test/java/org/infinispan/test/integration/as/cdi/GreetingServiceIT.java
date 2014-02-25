package org.infinispan.test.integration.as.cdi;

import org.infinispan.test.integration.as.category.UnstableTest;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.experimental.categories.Category;

import javax.cache.annotation.CacheKey;
import javax.inject.Inject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Kevin Pollet <pollet.kevin@gmail.com> (C) 2011
 */
@RunWith(Arquillian.class)
@Category(UnstableTest.class) // See ISPN-4058
public class GreetingServiceIT {

   @Deployment
   public static WebArchive deployment() {
      return Deployments.baseDeployment()
            .addClass(GreetingServiceIT.class);
   }

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
