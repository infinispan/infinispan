package org.infinispan.integrationtests.cdijcache.interceptor;

import static org.infinispan.integrationtests.cdijcache.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;

import javax.cache.CacheException;
import javax.cache.annotation.GeneratedCacheKey;
import javax.inject.Inject;

import org.infinispan.Cache;
import org.infinispan.cdi.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.integrationtests.cdijcache.interceptor.config.Config;
import org.infinispan.integrationtests.cdijcache.interceptor.config.Custom;
import org.infinispan.integrationtests.cdijcache.interceptor.service.CacheRemoveEntryService;
import org.infinispan.integrationtests.cdijcache.interceptor.service.CustomCacheKey;
import org.infinispan.jcache.annotation.DefaultCacheKey;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @see javax.cache.annotation.CacheRemoveEntry
 */
@Test(groups = "functional", testName = "cdi.test.interceptor.CacheRemoveEntryInterceptorTest")
public class CacheRemoveEntryInterceptorTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(CacheRemoveEntryInterceptorTest.class)
            .addClass(CacheRemoveEntryService.class)
            .addPackage(Config.class.getPackage())
            .addClass(DefaultTestEmbeddedCacheManagerProducer.class)
            .addAsWebInfResource(MethodHandles.lookup().lookupClass().getResource("/beans.xml"), "beans.xml");
   }

   @Inject
   private CacheRemoveEntryService service;

   @Inject
   @Custom
   private Cache<GeneratedCacheKey, String> customCache;

   @BeforeMethod
   public void beforeMethod() {
      customCache.clear();
      assertTrue(customCache.isEmpty());
   }

   @Test(expectedExceptions = CacheException.class)
   public void testCacheRemoveEntry() {
      service.removeEntry("Kevin");
   }

   public void testCacheRemoveEntryWithCacheName() {
      final GeneratedCacheKey cacheKey = new DefaultCacheKey(new Object[]{"Kevin"});

      customCache.put(cacheKey, "Hello Kevin");

      assertEquals(customCache.size(), 1);
      assertTrue(customCache.containsKey(cacheKey));

      service.removeEntryWithCacheName("Kevin");

      assertEquals(customCache.size(), 0);
   }

   public void testCacheRemoveEntryWithCacheKeyParam() {
      final GeneratedCacheKey cacheKey = new DefaultCacheKey(new Object[]{"Kevin"});

      customCache.put(cacheKey, "Hello Kevin");

      assertEquals(customCache.size(), 1);
      assertTrue(customCache.containsKey(cacheKey));

      service.removeEntryWithCacheKeyParam("Kevin", "foo");

      assertEquals(customCache.size(), 0);
   }

   public void testCacheRemoveEntryAfterInvocationWithException() {
      final GeneratedCacheKey cacheKey = new DefaultCacheKey(new Object[]{"Kevin"});

      customCache.put(cacheKey, "Hello Kevin");

      assertEquals(customCache.size(), 1);
      assertTrue(customCache.containsKey(cacheKey));

      try {

         service.removeEntryWithCacheName(null);

      } catch (NullPointerException e) {
         assertEquals(customCache.size(), 1);
      }
   }

   public void testCacheRemoveEntryWithCacheKeyGenerator() throws NoSuchMethodException {
      final Method method = CacheRemoveEntryService.class.getMethod("removeEntryWithCacheKeyGenerator", String.class);
      final GeneratedCacheKey cacheKey = new CustomCacheKey(method, "Kevin");

      customCache.put(cacheKey, "Hello Kevin");

      assertEquals(customCache.size(), 1);
      assertTrue(customCache.containsKey(cacheKey));

      service.removeEntryWithCacheKeyGenerator("Kevin");

      assertEquals(customCache.size(), 0);
   }

   public void testCacheRemoveEntryBeforeInvocationWithException() {
      final GeneratedCacheKey cacheKey = new DefaultCacheKey(new Object[]{"Kevin"});

      customCache.put(cacheKey, "Hello Kevin");

      assertEquals(customCache.size(), 1);
      assertTrue(customCache.containsKey(cacheKey));

      try {

         service.removeEntryBeforeInvocationWithException("Kevin");

      } catch (NullPointerException e) {
         assertEquals(customCache.size(), 0);
      }
   }
}
