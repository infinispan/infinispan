package org.infinispan.cdi.test.interceptor;

import org.infinispan.Cache;
import org.infinispan.cdi.test.interceptor.service.Custom;
import org.infinispan.cdi.test.interceptor.service.GreetingService;
import org.infinispan.cdi.test.interceptor.service.Small;
import org.infinispan.cdi.test.interceptor.service.generator.CustomCacheKey;
import org.infinispan.cdi.test.interceptor.service.generator.CustomCacheKeyGenerator;
import org.infinispan.manager.CacheContainer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.cache.interceptor.CacheKey;
import javax.inject.Inject;
import java.lang.reflect.Method;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.infinispan.cdi.util.CacheHelper.getDefaultMethodCacheName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * @author Kevin Pollet - SERLI - (kevin.pollet@serli.com)
 * @see javax.cache.interceptor.CacheResult
 */
public class CacheResultInterceptorTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addPackage(GreetingService.class.getPackage())
            .addPackage(CustomCacheKeyGenerator.class.getPackage())
            .addPackage(CacheResultInterceptorTest.class.getPackage());
   }

   @Inject
   private CacheContainer cacheContainer;

   @Inject
   private GreetingService greetingService;

   @Inject
   @Custom
   private Cache<CacheKey, String> customCache;

   @Inject
   @Small
   private Cache<CacheKey, String> smallCache;

   @BeforeMethod(groups = "functional")
   public void setUp() {
      customCache.clear();
      assertTrue(customCache.isEmpty());
   }

   @Test(groups = "functional")
   public void testDefaultCacheResult() throws NoSuchMethodException {
      Method method = GreetingService.class.getMethod("sayMorning", String.class);
      Cache<CacheKey, String> cache = cacheContainer.getCache(getDefaultMethodCacheName(method));

      String message = greetingService.sayMorning("Foo");
      assertEquals("Morning Foo", message);
      assertEquals(cache.size(), 1);

      message = greetingService.sayMorning("Foo");
      assertEquals("Morning Foo", message);
      assertEquals(cache.size(), 1);

      assertEquals(greetingService.getSayMorningCount(), 1);
   }

   @Test(groups = "functional")
   public void testCacheResultWithCacheName() {
      String message = greetingService.sayHi("Pete");

      assertNotNull(message);
      assertEquals("Hi Pete", message);
      assertEquals(customCache.size(), 1);

      message = greetingService.sayHi("Pete");
      assertNotNull(message);
      assertEquals("Hi Pete", message);
      assertEquals(customCache.size(), 1);

      assertEquals(greetingService.getSayHiCount(), 1);
   }

   @Test(groups = "functional")
   public void testCacheResultWithCustomCacheKeyGenerator() throws NoSuchMethodException {
      Method method = GreetingService.class.getMethod("sayHello", String.class);
      Cache<CacheKey, String> cache = cacheContainer.getCache(getDefaultMethodCacheName(method));

      String message = greetingService.sayHello("Kevin");
      assertEquals("Hello Kevin", message);
      assertEquals(cache.size(), 1);
      assertTrue(cache.containsKey(new CustomCacheKey(method, "Kevin")));

      message = greetingService.sayHello("Kevin");
      assertEquals("Hello Kevin", message);
      assertEquals(cache.size(), 1);

      assertEquals(greetingService.getSayHelloCount(), 1);
   }

   @Test(groups = "functional")
   public void testCacheResultWithSkipGet() throws NoSuchMethodException {
      Method method = GreetingService.class.getMethod("sayHey", String.class);
      Cache<CacheKey, String> cache = cacheContainer.getCache(getDefaultMethodCacheName(method));

      String message = greetingService.sayHey("Manik");

      assertNotNull(message);
      assertEquals("Hey Manik", message);
      assertEquals(cache.size(), 1);

      message = greetingService.sayHey("Manik");
      assertNotNull(message);
      assertEquals("Hey Manik", message);
      assertEquals(cache.size(), 1);

      assertEquals(greetingService.getSayHeyCount(), 2);
   }

   @Test(groups = "functional")
   public void testCacheResultWithSpecificCacheManager() {
      String message = greetingService.sayBonjour("Pete");

      assertNotNull(message);
      assertEquals("Bonjour Pete", message);
      assertEquals(smallCache.size(), 1);

      message = greetingService.sayBonjour("Pete");
      assertNotNull(message);
      assertEquals("Bonjour Pete", message);
      assertEquals(smallCache.size(), 1);

      assertEquals(greetingService.getSayBonjourCount(), 1);
      assertEquals(smallCache.size(), 1);
      assertEquals(smallCache.getConfiguration().getEvictionMaxEntries(), 4);
   }
}
