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
import org.infinispan.cdi.test.interceptor.service.CacheRemoveService;
import org.infinispan.cdi.test.interceptor.service.Custom;
import org.infinispan.cdi.test.interceptor.service.GreetingService;
import org.infinispan.cdi.test.interceptor.service.generator.CustomCacheKeyGenerator;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.cache.CacheException;
import javax.cache.interceptor.CacheKey;
import javax.cache.interceptor.DefaultCacheKey;
import javax.inject.Inject;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @see javax.cache.interceptor.CacheRemoveAll
 */
public class CacheRemoveAllInterceptorTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addPackage(GreetingService.class.getPackage())
            .addPackage(CustomCacheKeyGenerator.class.getPackage())
            .addPackage(CacheRemoveAllInterceptorTest.class.getPackage());
   }

   @Inject
   private CacheRemoveService adminService;

   @Inject
   @Custom
   private Cache<CacheKey, String> cache;

   @BeforeMethod(groups = "functional")
   public void setUp() {
      cache.clear();
      assertTrue(cache.isEmpty());
   }

   @Test(groups = "functional", expectedExceptions = CacheException.class)
   public void testDefaultCacheRemoveAll() {
      cache.put(new DefaultCacheKey(new Object[]{"Kevin"}), "Hi Kevin");
      cache.put(new DefaultCacheKey(new Object[]{"Pete"}), "Hi Pete");
      assertEquals(cache.size(), 2);

      adminService.removeAll();
      assertEquals(cache.size(), 0);
   }

   @Test(groups = "functional")
   public void testCacheRemoveAllWithCacheName() {
      cache.put(new DefaultCacheKey(new Object[]{"Kevin"}), "Hi Kevin");
      cache.put(new DefaultCacheKey(new Object[]{"Pete"}), "Hi Pete");
      assertEquals(cache.size(), 2);

      adminService.removeAllWithCacheName();
      assertEquals(cache.size(), 0);
   }

   @Test(groups = "functional")
   public void testCacheRemoveAllAfterInvocationWithException() {
      cache.put(new DefaultCacheKey(new Object[]{"Kevin"}), "Hi Kevin");
      cache.put(new DefaultCacheKey(new Object[]{"Pete"}), "Hi Pete");
      assertEquals(cache.size(), 2);

      try {

         adminService.removeAllAfterInvocationWithException();

      } catch (IllegalArgumentException e) {
         assertEquals(cache.size(), 2);
      }
   }

   @Test(groups = "functional")
   public void testCacheRemoveAllBeforeInvocationWithException() {
      cache.put(new DefaultCacheKey(new Object[]{"Kevin"}), "Hi Kevin");
      cache.put(new DefaultCacheKey(new Object[]{"Pete"}), "Hi Pete");
      assertEquals(cache.size(), 2);

      try {

         adminService.removeAllBeforeInvocationWithException();

      } catch (IllegalArgumentException e) {
         assertEquals(cache.size(), 0);
      }
   }
}
