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
package org.jboss.seam.infinispan.test.interceptors;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.jboss.arquillian.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.seam.infinispan.test.interceptors.service.CacheRemoveService;
import org.jboss.seam.infinispan.test.interceptors.service.Custom;
import org.jboss.seam.infinispan.test.interceptors.service.GreetingService;
import org.jboss.seam.infinispan.test.interceptors.service.generator.CustomCacheKey;
import org.jboss.seam.infinispan.test.interceptors.service.generator.CustomCacheKeyGenerator;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.cache.interceptor.CacheKey;
import javax.cache.interceptor.DefaultCacheKey;
import javax.inject.Inject;
import java.lang.reflect.Method;

import static org.jboss.seam.infinispan.test.testutil.Deployments.baseDeployment;
import static org.jboss.seam.infinispan.util.CacheHelper.getDefaultMethodCacheName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @see javax.cache.interceptor.CacheRemoveEntry
 */
public class CacheRemoveEntryInterceptorTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addPackage(GreetingService.class.getPackage())
            .addPackage(CustomCacheKeyGenerator.class.getPackage())
            .addPackage(CacheRemoveEntryInterceptorTest.class.getPackage());
   }

   @Inject
   private CacheContainer cacheContainer;

   @Inject
   private CacheRemoveService adminService;

   @Inject
   @Custom
   private Cache<CacheKey, Boolean> customCache;

   @Test(groups = "functional")
   public void testDefaultCacheRemoveEntry() throws NoSuchMethodException {
      Method method = CacheRemoveService.class.getMethod("removeUser", String.class);
      Cache<CacheKey, String> cache = cacheContainer.getCache(getDefaultMethodCacheName(method));

      CacheKey cacheKey = new DefaultCacheKey(new Object[]{"Kevin"});
      cache.put(cacheKey, "Hello Kevin");
      assertEquals(cache.size(), 1);
      assertTrue(cache.containsKey(cacheKey));

      adminService.removeUser("Kevin");
      assertEquals(cache.size(), 0);
   }

   @Test(groups = "functional")
   public void testCacheRemoveEntryAfterInvocationWithException() throws NoSuchMethodException {
      Method method = CacheRemoveService.class.getMethod("removeUser", String.class);
      Cache<CacheKey, String> cache = cacheContainer.getCache(getDefaultMethodCacheName(method));

      CacheKey cacheKey = new DefaultCacheKey(new Object[]{"Kevin"});
      cache.put(cacheKey, "Hello Kevin");
      assertEquals(cache.size(), 1);
      assertTrue(cache.containsKey(cacheKey));

      try {

         adminService.removeUser("Kevin");

      } catch (IllegalArgumentException e) {
         assertEquals(cache.size(), 1);
      }
   }

   @Test(groups = "functional")
   public void testCacheRemoveEntryWithCustomCacheKeyGenerator() throws NoSuchMethodException {
      Method method = CacheRemoveService.class.getMethod("removeUserWithCustomCacheKeyGenerator", String.class);
      Cache<CacheKey, String> cache = cacheContainer.getCache(getDefaultMethodCacheName(method));

      CacheKey cacheKey = new CustomCacheKey(method, "Kevin");
      cache.put(cacheKey, "Hello Kevin");
      assertEquals(cache.size(), 1);
      assertTrue(cache.containsKey(cacheKey));

      adminService.removeUserWithCustomCacheKeyGenerator("Kevin");
      assertEquals(cache.size(), 0);
   }

   @Test(groups = "functional")
   public void testCacheRemoveEntryBeforeInvocationWithException() throws NoSuchMethodException {
      Method method = CacheRemoveService.class.getMethod("removeUserBeforeInvocationWithException", String.class);
      Cache<CacheKey, String> cache = cacheContainer.getCache(getDefaultMethodCacheName(method));

      CacheKey cacheKey = new DefaultCacheKey(new Object[]{"Kevin"});
      cache.put(cacheKey, "Hello Kevin");
      assertEquals(cache.size(), 1);
      assertTrue(cache.containsKey(cacheKey));

      try {

         adminService.removeUserBeforeInvocationWithException("Kevin");

      } catch (IllegalArgumentException e) {
         assertEquals(cache.size(), 0);
      }
   }

   @Test(groups = "functional")
   public void testCacheRemoveEntryWithCacheName() {
      CacheKey cacheKey = new DefaultCacheKey(new Object[]{"Kevin"});
      customCache.put(cacheKey, true);
      assertEquals(customCache.size(), 1);
      assertTrue(customCache.containsKey(cacheKey));

      adminService.removeUserWithCacheName("Kevin");
      assertEquals(customCache.size(), 0);
   }
}
