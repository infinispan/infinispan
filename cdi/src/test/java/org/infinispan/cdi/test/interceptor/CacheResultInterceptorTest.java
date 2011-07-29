package org.infinispan.cdi.test.interceptor;

import org.infinispan.Cache;
import org.infinispan.cdi.test.interceptor.service.Custom;
import org.infinispan.cdi.test.interceptor.service.CustomCacheKey;
import org.infinispan.cdi.test.interceptor.service.CustomCacheKeyGenerator;
import org.infinispan.cdi.test.interceptor.service.GreetingService;
import org.infinispan.cdi.test.interceptor.service.Small;
import org.infinispan.manager.EmbeddedCacheManager;

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
@Test(groups = "functional", testName = "cdi.test.interceptor.CacheResultInterceptorTest")
public class CacheResultInterceptorTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addPackage(GreetingService.class.getPackage())
            .addPackage(CustomCacheKeyGenerator.class.getPackage())
            .addPackage(CacheResultInterceptorTest.class.getPackage());
   }

   @Inject
   private EmbeddedCacheManager cacheManager;

   @Inject
   private GreetingService service;

   @Inject
   @Custom
   private Cache<CacheKey, String> customCache;

   @Inject
   @Small
   private Cache<CacheKey, String> smallCache;

   @BeforeMethod
   public void setUp() {
      customCache.clear();
      assertTrue(customCache.isEmpty());
   }

   public void testDefaultCacheResult() throws NoSuchMethodException {
      Method method = GreetingService.class.getMethod("sayMorning", String.class);
      Cache<CacheKey, String> cache = cacheManager.getCache(getDefaultMethodCacheName(method));

      String message = service.sayMorning("Foo");
      assertEquals("Morning Foo", message);
      assertEquals(cache.size(), 1);

      message = service.sayMorning("Foo");
      assertEquals("Morning Foo", message);
      assertEquals(cache.size(), 1);

      assertEquals(service.getSayMorningCount(), 1);
   }

   public void testCacheResultWithCacheName() {
      String message = service.sayHi("Pete");

      assertNotNull(message);
      assertEquals("Hi Pete", message);
      assertEquals(customCache.size(), 1);

      message = service.sayHi("Pete");
      assertNotNull(message);
      assertEquals("Hi Pete", message);
      assertEquals(customCache.size(), 1);

      assertEquals(service.getSayHiCount(), 1);
   }

   public void testCacheResultWithCustomCacheKeyGenerator() throws NoSuchMethodException {
      Method method = GreetingService.class.getMethod("sayHello", String.class);
      Cache<CacheKey, String> cache = cacheManager.getCache(getDefaultMethodCacheName(method));

      String message = service.sayHello("Kevin");
      assertEquals("Hello Kevin", message);
      assertEquals(cache.size(), 1);
      assertTrue(cache.containsKey(new CustomCacheKey(method, "Kevin")));

      message = service.sayHello("Kevin");
      assertEquals("Hello Kevin", message);
      assertEquals(cache.size(), 1);

      assertEquals(service.getSayHelloCount(), 1);
   }

   public void testCacheResultWithSkipGet() throws NoSuchMethodException {
      Method method = GreetingService.class.getMethod("sayHey", String.class);
      Cache<CacheKey, String> cache = cacheManager.getCache(getDefaultMethodCacheName(method));

      String message = service.sayHey("Manik");

      assertNotNull(message);
      assertEquals("Hey Manik", message);
      assertEquals(cache.size(), 1);

      message = service.sayHey("Manik");
      assertNotNull(message);
      assertEquals("Hey Manik", message);
      assertEquals(cache.size(), 1);

      assertEquals(service.getSayHeyCount(), 2);
   }

   public void testCacheResultWithSpecificCacheManager() {
      String message = service.sayBonjour("Pete");

      assertNotNull(message);
      assertEquals("Bonjour Pete", message);
      assertEquals(smallCache.size(), 1);

      message = service.sayBonjour("Pete");
      assertNotNull(message);
      assertEquals("Bonjour Pete", message);
      assertEquals(smallCache.size(), 1);

      assertEquals(service.getSayBonjourCount(), 1);
      assertEquals(smallCache.size(), 1);
      assertEquals(smallCache.getConfiguration().getEvictionMaxEntries(), 4);
   }
}
