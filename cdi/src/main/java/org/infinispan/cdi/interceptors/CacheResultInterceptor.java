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
package org.infinispan.cdi.interceptors;

import org.infinispan.Cache;

import javax.cache.interceptor.CacheKey;
import javax.cache.interceptor.CacheResult;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import java.lang.reflect.Method;

import static org.infinispan.cdi.util.CacheHelper.generateCacheKey;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Interceptor
@CacheResult
public class CacheResultInterceptor {

   private final InfinispanCacheResolver cacheResolver;

   @Inject
   public CacheResultInterceptor(InfinispanCacheResolver cacheResolver) {
      this.cacheResolver = cacheResolver;
   }

   @AroundInvoke
   public Object cacheResult(InvocationContext context) throws Exception {
      final CacheResult cacheResult = retrieveCacheResultAnnotation(context);
      final Cache<CacheKey, Object> cache = cacheResolver.resolveCache(cacheResult.cacheName(), context.getMethod());
      final CacheKey cacheKey = generateCacheKey(cacheResult.cacheKeyGenerator(), context);

      Object methodResult = null;

      if (!cacheResult.skipGet()) {
         methodResult = cache.get(cacheKey);
      }

      if (methodResult == null) {
         methodResult = context.proceed();
         if (methodResult != null) {
            cache.put(cacheKey, methodResult);
         }
      }

      return methodResult;
   }

   private CacheResult retrieveCacheResultAnnotation(InvocationContext context) {
      final Method method = context.getMethod();
      final Class<?> declaringClass = method.getDeclaringClass();

      if (method.isAnnotationPresent(CacheResult.class)) {
         return method.getAnnotation(CacheResult.class);
      }
      return declaringClass.getAnnotation(CacheResult.class);
   }
}
