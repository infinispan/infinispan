package org.infinispan.integrationtests.cdijcache.interceptor;

import org.infinispan.Cache;
import org.infinispan.cdi.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.integrationtests.cdijcache.interceptor.config.Config;
import org.infinispan.integrationtests.cdijcache.interceptor.config.Custom;
import org.infinispan.integrationtests.cdijcache.interceptor.service.CacheRemoveAllService;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.cache.CacheException;
import javax.inject.Inject;
import java.lang.invoke.MethodHandles;

import static org.infinispan.integrationtests.cdijcache.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @see javax.cache.annotation.CacheRemoveAll
 */
@Test(groups = "functional", testName = "cdi.test.interceptor.CacheRemoveAllInterceptorTest")
public class CacheRemoveAllInterceptorTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(CacheRemoveAllInterceptorTest.class)
            .addClass(CacheRemoveAllService.class)
            .addPackage(Config.class.getPackage())
            .addClass(DefaultTestEmbeddedCacheManagerProducer.class)
            .addAsWebInfResource(MethodHandles.lookup().lookupClass().getResource("/beans.xml"), "beans.xml");
   }

   @Inject
   private CacheRemoveAllService service;

   @Inject
   @Custom
   private Cache<String, String> customCache;

   @BeforeMethod
   public void beforeMethod() {
      customCache.clear();
      assertTrue(customCache.isEmpty());
   }

   @Test(expectedExceptions = CacheException.class)
   public void testCacheRemoveAll() {
      customCache.put("Kevin", "Hi Kevin");
      customCache.put("Pete", "Hi Pete");

      assertEquals(customCache.size(), 2);

      service.removeAll();

      assertEquals(customCache.size(), 0);
   }

   public void testCacheRemoveAllWithCacheName() {
      customCache.put("Kevin", "Hi Kevin");
      customCache.put("Pete", "Hi Pete");

      assertEquals(customCache.size(), 2);

      service.removeAllWithCacheName();

      assertEquals(customCache.size(), 0);
   }

   public void testCacheRemoveAllAfterInvocationWithException() {
      customCache.put("Kevin", "Hi Kevin");
      customCache.put("Pete", "Hi Pete");

      assertEquals(customCache.size(), 2);

      try {

         service.removeAllAfterInvocationWithException();

      } catch (RuntimeException e) {
         assertEquals(customCache.size(), 2);
      }
   }

   public void testCacheRemoveAllBeforeInvocationWithException() {
      customCache.put("Kevin", "Hi Kevin");
      customCache.put("Pete", "Hi Pete");

      assertEquals(customCache.size(), 2);

      try {

         service.removeAllBeforeInvocationWithException();

      } catch (RuntimeException e) {
         assertEquals(customCache.size(), 0);
      }
   }
}
