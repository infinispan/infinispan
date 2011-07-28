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
package org.infinispan.cdi.interceptor;

import org.infinispan.Cache;

import javax.cache.CacheException;
import javax.cache.interceptor.CacheKey;
import javax.cache.interceptor.CacheRemoveAll;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import java.lang.reflect.Method;

/**
 * <p>Implementation class of the {@link CacheRemoveAll} interceptor. This interceptor uses the following algorithm
 * describes in JSR-107.</p>
 *
 * <p>The interceptor that intercepts method annotated with {@code @CacheRemoveAll} must do the following, remove all
 * entries associated with the cache. The removeAll occurs after the method body is executed. This can be overridden by
 * specifying a afterInvocation attribute value of false. If afterInvocation is true and the annotated method throws an
 * exception, the removeAll will not happen.</p>
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Interceptor
public class CacheRemoveAllInterceptor {

   private final CacheResolver cacheResolver;

   @Inject
   public CacheRemoveAllInterceptor(CacheResolver cacheResolver) {
      this.cacheResolver = cacheResolver;
   }

   @AroundInvoke
   public Object cacheRemoveAll(InvocationContext context) throws Exception {
      final Method method = context.getMethod();
      final CacheRemoveAll cacheRemoveAll = method.getAnnotation(CacheRemoveAll.class);
      final String cacheName = cacheRemoveAll.cacheName();

      //TODO KP: is it the expected behavior? nothing about this in the spec.
      if (cacheName.trim().isEmpty()) {
         throw new CacheException("Method named '" + method.getName() + "' annotated with CacheRemoveAll " +
                                        "doesn't specify a cache name");
      }

      final Cache<CacheKey, Object> cache = cacheResolver.resolveCache(cacheName, method);

      if (!cacheRemoveAll.afterInvocation()) {
         cache.clear();
      }

      final Object result = context.proceed();

      if (cacheRemoveAll.afterInvocation()) {
         cache.clear();
      }

      return result;
   }
}
