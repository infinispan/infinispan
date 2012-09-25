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
import org.infinispan.cdi.interceptor.DefaultCacheKey;
import org.infinispan.cdi.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.cdi.test.interceptor.config.Config;
import org.infinispan.cdi.test.interceptor.config.Custom;
import org.infinispan.cdi.test.interceptor.service.CacheRemoveEntryService;
import org.infinispan.cdi.test.interceptor.service.CustomCacheKey;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.cache.CacheException;
import javax.cache.annotation.CacheKey;
import javax.inject.Inject;
import java.lang.reflect.Method;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
            .addClass(DefaultTestEmbeddedCacheManagerProducer.class);
   }

   @Inject
   private CacheRemoveEntryService service;

   @Inject
   @Custom
   private Cache<CacheKey, String> customCache;

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
      final CacheKey cacheKey = new DefaultCacheKey(new Object[]{"Kevin"});

      customCache.put(cacheKey, "Hello Kevin");

      assertEquals(customCache.size(), 1);
      assertTrue(customCache.containsKey(cacheKey));

      service.removeEntryWithCacheName("Kevin");

      assertEquals(customCache.size(), 0);
   }

   public void testCacheRemoveEntryWithCacheKeyParam() {
      final CacheKey cacheKey = new DefaultCacheKey(new Object[]{"Kevin"});

      customCache.put(cacheKey, "Hello Kevin");

      assertEquals(customCache.size(), 1);
      assertTrue(customCache.containsKey(cacheKey));

      service.removeEntryWithCacheKeyParam("Kevin", "foo");

      assertEquals(customCache.size(), 0);
   }

   public void testCacheRemoveEntryAfterInvocationWithException() {
      final CacheKey cacheKey = new DefaultCacheKey(new Object[]{"Kevin"});

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
      final CacheKey cacheKey = new CustomCacheKey(method, "Kevin");

      customCache.put(cacheKey, "Hello Kevin");

      assertEquals(customCache.size(), 1);
      assertTrue(customCache.containsKey(cacheKey));

      service.removeEntryWithCacheKeyGenerator("Kevin");

      assertEquals(customCache.size(), 0);
   }

   public void testCacheRemoveEntryBeforeInvocationWithException() {
      final CacheKey cacheKey = new DefaultCacheKey(new Object[]{"Kevin"});

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
