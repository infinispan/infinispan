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
import org.infinispan.cdi.test.interceptor.service.CacheRemoveAllService;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.cache.CacheException;
import javax.inject.Inject;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
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
            .addPackage(Config.class.getPackage());
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
