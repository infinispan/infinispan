/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.cdi.test.interceptor;

import org.infinispan.Cache;
import org.infinispan.cdi.test.interceptor.config.Config;
import org.infinispan.cdi.test.interceptor.config.Custom;
import org.infinispan.cdi.test.interceptor.config.Small;
import org.infinispan.cdi.test.interceptor.service.CacheResultService;
import org.infinispan.cdi.test.interceptor.service.CustomCacheKey;
import org.infinispan.manager.CacheContainer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.cache.annotation.CacheKey;
import javax.inject.Inject;
import java.lang.reflect.Method;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * @author Kevin Pollet - SERLI - (kevin.pollet@serli.com)
 * @see javax.cache.annotation.CacheResult
 */
@Test(groups = "functional", testName = "cdi.test.interceptor.CacheResultInterceptorTest")
public class CacheResultInterceptorTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(CacheResultInterceptorTest.class)
            .addClass(CacheResultService.class)
            .addPackage(Config.class.getPackage());
   }

   @Inject
   private CacheContainer cacheContainer;

   @Inject
   private CacheResultService service;

   @Inject
   @Custom
   private Cache<CacheKey, String> customCache;

   @Inject
   @Small
   private Cache<CacheKey, String> smallCache;

   @BeforeMethod
   public void beforeMethod() {
      customCache.clear();
      assertTrue(customCache.isEmpty());
   }

   public void testCacheResult() throws NoSuchMethodException {
      final StringBuilder cacheName = new StringBuilder()
            .append(CacheResultService.class.getName())
            .append(".cacheResult(java.lang.String)");

      final Cache<CacheKey, String> cache = cacheContainer.getCache(cacheName.toString());

      String message = service.cacheResult("Foo");

      assertEquals("Morning Foo", message);
      assertEquals(cache.size(), 1);

      message = service.cacheResult("Foo");

      assertEquals("Morning Foo", message);
      assertEquals(cache.size(), 1);

      assertEquals(service.getNbCall(), 1);
   }

   public void testCacheResultWithCacheName() {
      String message = service.cacheResultWithCacheName("Pete");

      assertNotNull(message);
      assertEquals("Hi Pete", message);
      assertEquals(customCache.size(), 1);

      message = service.cacheResultWithCacheName("Pete");

      assertNotNull(message);
      assertEquals("Hi Pete", message);
      assertEquals(customCache.size(), 1);

      assertEquals(service.getNbCall(), 1);
   }

   public void testCacheResultWithCacheKeyParam() {
      String message = service.cacheResultWithCacheKeyParam("Pete", "foo");

      assertNotNull(message);
      assertEquals("Hola Pete", message);
      assertEquals(customCache.size(), 1);

      message = service.cacheResultWithCacheKeyParam("Pete", "foo2");

      assertNotNull(message);
      assertEquals("Hola Pete", message);
      assertEquals(customCache.size(), 1);

      assertEquals(service.getNbCall(), 1);
   }

   public void testCacheResultWithCustomCacheKeyGenerator() throws NoSuchMethodException {
      final Method method = CacheResultService.class.getMethod("cacheResultWithCacheKeyGenerator", String.class);

      String message = service.cacheResultWithCacheKeyGenerator("Kevin");

      assertEquals("Hello Kevin", message);
      assertEquals(customCache.size(), 1);
      assertTrue(customCache.containsKey(new CustomCacheKey(method, "Kevin")));

      message = service.cacheResultWithCacheKeyGenerator("Kevin");

      assertEquals("Hello Kevin", message);
      assertEquals(customCache.size(), 1);

      assertEquals(service.getNbCall(), 1);
   }

   public void testCacheResultWithSkipGet() throws NoSuchMethodException {
      String message = service.cacheResultSkipGet("Manik");

      assertNotNull(message);
      assertEquals("Hey Manik", message);
      assertEquals(customCache.size(), 1);

      message = service.cacheResultSkipGet("Manik");

      assertNotNull(message);
      assertEquals("Hey Manik", message);
      assertEquals(customCache.size(), 1);

      assertEquals(service.getNbCall(), 2);
   }

   public void testCacheResultWithSpecificCacheManager() {
      String message = service.cacheResultWithSpecificCacheManager("Pete");

      assertNotNull(message);
      assertEquals("Bonjour Pete", message);
      assertEquals(smallCache.size(), 1);

      message = service.cacheResultWithSpecificCacheManager("Pete");

      assertNotNull(message);
      assertEquals("Bonjour Pete", message);
      assertEquals(smallCache.size(), 1);

      assertEquals(service.getNbCall(), 1);
      assertEquals(smallCache.size(), 1);
      assertEquals(smallCache.getCacheConfiguration().eviction().maxEntries(), 4);
   }
}
