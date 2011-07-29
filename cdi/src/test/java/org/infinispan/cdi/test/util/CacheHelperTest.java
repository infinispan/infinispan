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
package org.infinispan.cdi.test.util;

import org.infinispan.cdi.interceptor.DefaultCacheKey;
import org.infinispan.cdi.interceptor.DefaultCacheKeyGenerator;
import org.infinispan.cdi.util.CacheHelper;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.cache.interceptor.CacheKey;
import javax.interceptor.InvocationContext;
import java.lang.reflect.Method;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.verify;
import static org.easymock.classextension.EasyMock.replay;
import static org.testng.Assert.assertEquals;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Test(groups = "unit", testName = "cdi.test.util.CacheHelperTest")
public class CacheHelperTest {

   private InvocationContext contextMock;

   @BeforeClass
   public void setUp() throws Exception {
      contextMock = createMock(InvocationContext.class);
      expect(contextMock.getParameters()).andReturn(new String[]{"first, second"});
   }

   @Test(expectedExceptions = NullPointerException.class,
         expectedExceptionsMessageRegExp = "method parameter cannot be null")
   public void testGetDefaultMethodCacheNameWithNullParameter() throws Exception {
      CacheHelper.getDefaultMethodCacheName(null);
   }

   public void testGetDefaultMethodCacheName() throws Exception {
      Method method = CacheHelperTest.class.getMethod("fooMethod", Integer.TYPE, String.class);
      String defaultCacheName = CacheHelper.getDefaultMethodCacheName(method);

      assertEquals(defaultCacheName, "org.infinispan.cdi.test.util.CacheHelperTest.fooMethod(int,java.lang.String)");
   }

   @Test(expectedExceptions = NullPointerException.class,
         expectedExceptionsMessageRegExp = "cacheKeyGeneratorClass parameter cannot be null")
   public void testGenerateCacheKeyWithNullCacheKeyGeneratorClass() throws Exception {
      CacheHelper.generateCacheKey(null, contextMock);
   }

   @Test(expectedExceptions = NullPointerException.class,
         expectedExceptionsMessageRegExp = "context parameter cannot be null")
   public void testGenerateCacheKeyWithNullInvocationContext() throws Exception {
      CacheHelper.generateCacheKey(DefaultCacheKeyGenerator.class, null);
   }

   public void testGenerateCacheKey() throws Exception {
      replay(contextMock);
      CacheKey cacheKey = CacheHelper.generateCacheKey(DefaultCacheKeyGenerator.class, contextMock);

      assertEquals(cacheKey, new DefaultCacheKey(new String[]{"first, second"}));
      verify(contextMock);
   }

   public String fooMethod(int first, String second) {
      return second + first;
   }
}
