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

import javax.cache.interceptor.CacheKey;
import javax.cache.interceptor.CacheResult;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import java.lang.reflect.Method;

import static org.infinispan.cdi.util.CacheHelper.generateCacheKey;
import static org.infinispan.cdi.util.CacheHelper.getDefaultMethodCacheName;

/**
 * <p>Implementation class of the {@link CacheResult} interceptor. This interceptor uses the following algorithm
 * describes in JSR-107.</p>
 *
 * <p>When a method annotated with @CacheResult is invoked the following must occur.
 * <ol>
 *    <li>Generate a key based on InvocationContext using the specified {@linkplain javax.cache.interceptor.CacheKeyGenerator
 *    CacheKeyGenerator}.</li>
 *    <li>Use this key to look up the entry in the cache.</li>
 *    <li>If an entry is found return it as the result and do not call the annotated method.</li>
 *    <li>If no entry is found invoke the method.</li>
 *    <li>Use the result to populate the cache with this key/result pair.</li>
 * </ol>
 *
 * There is a skipGet attribute which if set to true will cause the method body to always be invoked and the return
 * value put into the cache. The cache is not checked for the key before method body invocation, skipping steps 2 and 3
 * from the list above. This can be used for annotating methods that do a cache.put() with no other consequences.</p>
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Interceptor
public class CacheResultInterceptor {

   private final CacheResolver cacheResolver;

   @Inject
   public CacheResultInterceptor(CacheResolver cacheResolver) {
      this.cacheResolver = cacheResolver;
   }

   @AroundInvoke
   public Object cacheResult(InvocationContext context) throws Exception {
      final Method method = context.getMethod();
      final CacheResult cacheResult = method.getAnnotation(CacheResult.class);
      final String cacheName = cacheResult.cacheName().trim().isEmpty() ? getDefaultMethodCacheName(method) : cacheResult.cacheName();
      final Cache<CacheKey, Object> cache = cacheResolver.resolveCache(cacheName, context.getMethod());
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
}
