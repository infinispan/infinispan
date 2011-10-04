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
import org.infinispan.cdi.interceptor.context.CacheKeyInvocationContextFactory;
import org.infinispan.cdi.interceptor.context.CacheKeyInvocationContextImpl;

import javax.cache.interceptor.CacheKey;
import javax.cache.interceptor.CacheKeyGenerator;
import javax.cache.interceptor.CacheKeyInvocationContext;
import javax.cache.interceptor.CacheResult;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import java.io.Serializable;

/**
 * <p>{@link CacheResult} interceptor implementation. This interceptor uses the following algorithm describes in
 * JSR-107.</p>
 *
 * <p>When a method annotated with {@link CacheResult} is invoked the following must occur.
 * <ol>
 *    <li>Generate a key based on InvocationContext using the specified {@linkplain CacheKeyGenerator}.</li>
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
public class CacheResultInterceptor implements Serializable {

   private static final long serialVersionUID = 5275055951121834315L;

   private final CacheResolver cacheResolver;
   private final CacheKeyInvocationContextFactory contextFactory;

   @Inject
   public CacheResultInterceptor(CacheResolver cacheResolver, CacheKeyInvocationContextFactory contextFactory) {
      this.cacheResolver = cacheResolver;
      this.contextFactory = contextFactory;
   }

   @AroundInvoke
   public Object cacheResult(InvocationContext invocationContext) throws Exception {
      final CacheKeyInvocationContext<CacheResult> cacheKeyInvocationContext = contextFactory.getCacheKeyInvocationContext(invocationContext);
      final CacheKeyGenerator cacheKeyGenerator = cacheKeyInvocationContext.unwrap(CacheKeyInvocationContextImpl.class).getCacheKeyGenerator();
      final CacheResult cacheResult = cacheKeyInvocationContext.getCacheAnnotation();
      final CacheKey cacheKey = cacheKeyGenerator.generateCacheKey((CacheKeyInvocationContext) cacheKeyInvocationContext);
      final Cache<CacheKey, Object> cache = cacheResolver.resolveCache(cacheKeyInvocationContext);

      Object result = null;

      if (!cacheResult.skipGet()) {
         result = cache.get(cacheKey);
      }

      if (result == null) {
         result = invocationContext.proceed();
         if (result != null) {
            cache.put(cacheKey, result);
         }
      }

      return result;
   }
}
