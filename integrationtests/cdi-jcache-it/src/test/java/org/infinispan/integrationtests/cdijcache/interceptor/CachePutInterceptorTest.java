package org.infinispan.integrationtests.cdijcache.interceptor;

import org.infinispan.Cache;
import org.infinispan.cdi.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.integrationtests.cdijcache.interceptor.config.Config;
import org.infinispan.integrationtests.cdijcache.interceptor.config.Custom;
import org.infinispan.integrationtests.cdijcache.interceptor.service.CachePutService;
import org.infinispan.integrationtests.cdijcache.interceptor.service.CustomCacheKey;
import org.infinispan.jcache.annotation.DefaultCacheKey;
import org.infinispan.manager.CacheContainer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.cache.annotation.CacheKey;
import javax.inject.Inject;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;

import static org.infinispan.integrationtests.cdijcache.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @see javax.cache.annotation.CachePut
 */
@Test(groups = "functional", testName = "cdi.test.interceptor.CachePutInterceptorTest")
public class CachePutInterceptorTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(CachePutInterceptorTest.class)
            .addClass(CachePutService.class)
            .addPackage(Config.class.getPackage())
            .addClass(DefaultTestEmbeddedCacheManagerProducer.class)
            .addAsWebInfResource(MethodHandles.lookup().lookupClass().getResource("/beans.xml"), "beans.xml");
   }

   @Inject
   private CachePutService service;

   @Inject
   private CacheContainer cacheContainer;

   @Custom
   @Inject
   private Cache<CacheKey, String> customCache;

   @BeforeMethod
   public void beforeMethod() {
      customCache.clear();
      assertTrue(customCache.isEmpty());
   }

   public void testCachePut() {
      final StringBuilder cacheName = new StringBuilder()
            .append(CachePutService.class.getName())
            .append(".put(long,java.lang.String)");

      final Cache<CacheKey, String> cache = cacheContainer.getCache(cacheName.toString());

      service.put(0l, "Manik");
      service.put(0l, "Kevin");
      service.put(1l, "Pete");

      assertEquals(cache.size(), 2);
      assertTrue(cache.containsKey(new DefaultCacheKey(new Object[]{0l})));
      assertTrue(cache.containsKey(new DefaultCacheKey(new Object[]{1l})));
   }

   public void testCachePutWithCacheName() {
      service.putWithCacheName(0l, "Manik");
      service.putWithCacheName(0l, "Kevin");
      service.putWithCacheName(1l, "Pete");

      assertEquals(customCache.size(), 2);
      assertTrue(customCache.containsKey(new DefaultCacheKey(new Object[]{0l})));
      assertTrue(customCache.containsKey(new DefaultCacheKey(new Object[]{1l})));
   }

   public void testCachePutCacheKeyParam() {
      service.putWithCacheKeyParam(0l, 1l, "Manik");
      service.putWithCacheKeyParam(0l, 1l, "Kevin");
      service.putWithCacheKeyParam(1l, 2l, "Pete");

      assertEquals(customCache.size(), 2);
      assertTrue(customCache.containsKey(new DefaultCacheKey(new Object[]{0l})));
      assertTrue(customCache.containsKey(new DefaultCacheKey(new Object[]{1l})));
   }

   public void testCachePutBeforeInvocation() {
      try {

         service.putBeforeInvocation(0l, "Manik");

      } catch (RuntimeException e) {
         assertEquals(customCache.size(), 1);
      }
   }

   public void putWithCacheKeyGenerator() throws Exception {
      final Method method = CachePutService.class.getMethod("putWithCacheKeyGenerator", Long.TYPE, String.class);

      service.putWithCacheKeyGenerator(0l, "Manik");
      service.putWithCacheKeyGenerator(0l, "Kevin");
      service.putWithCacheKeyGenerator(1l, "Pete");

      assertEquals(customCache.size(), 2);
      assertTrue(customCache.containsKey(new CustomCacheKey(method, 0l)));
      assertTrue(customCache.containsKey(new CustomCacheKey(method, 0l)));
   }
}
