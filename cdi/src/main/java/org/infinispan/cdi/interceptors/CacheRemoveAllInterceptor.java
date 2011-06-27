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
import javax.cache.interceptor.CacheRemoveAll;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import java.lang.reflect.Method;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Interceptor
@CacheRemoveAll
public class CacheRemoveAllInterceptor {

   private final InfinispanCacheResolver cacheResolver;

   @Inject
   public CacheRemoveAllInterceptor(InfinispanCacheResolver cacheResolver) {
      this.cacheResolver = cacheResolver;
   }

   @AroundInvoke
   public Object cacheRemoveAll(InvocationContext context) throws Exception {
      final Method method = context.getMethod();
      final CacheRemoveAll cacheRemoveEntry = method.getAnnotation(CacheRemoveAll.class);
      final Cache<CacheKey, Object> cache = cacheResolver.resolveCache(cacheRemoveEntry.cacheName(), method);

      if (!cacheRemoveEntry.afterInvocation()) {
         cache.clear();
      }

      final Object result = context.proceed();

      if (cacheRemoveEntry.afterInvocation()) {
         cache.clear();
      }

      return result;
   }
}
