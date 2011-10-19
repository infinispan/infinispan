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

import javax.cache.annotation.CacheKey;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.cache.annotation.CachePut;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import java.io.Serializable;

/**
 * {@link CachePut} interceptor implementation.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Interceptor
public class CachePutInterceptor implements Serializable {

   private static final long serialVersionUID = 270924196162168618L;

   private final CacheResolver cacheResolver;
   private final CacheKeyInvocationContextFactory contextFactory;

   @Inject
   public CachePutInterceptor(CacheResolver cacheResolver, CacheKeyInvocationContextFactory contextFactory) {
      this.cacheResolver = cacheResolver;
      this.contextFactory = contextFactory;
   }

   @AroundInvoke
   public Object cachePut(InvocationContext invocationContext) throws Exception {
      final CacheKeyInvocationContext<CachePut> cacheKeyInvocationContext = contextFactory.getCacheKeyInvocationContext(invocationContext);
      final CacheKeyGenerator cacheKeyGenerator = cacheKeyInvocationContext.unwrap(CacheKeyInvocationContextImpl.class).getCacheKeyGenerator();
      final CachePut cachePut = cacheKeyInvocationContext.getCacheAnnotation();
      final CacheKey cacheKey = cacheKeyGenerator.generateCacheKey(cacheKeyInvocationContext);
      final Cache<CacheKey, Object> cache = cacheResolver.resolveCache(cacheKeyInvocationContext);

      final Object valueToCache = cacheKeyInvocationContext.getValueParameter().getValue();

      if (!cachePut.afterInvocation() && valueToCache != null) {
         cache.put(cacheKey, valueToCache);
      }

      final Object result = invocationContext.proceed();

      if (cachePut.afterInvocation() && valueToCache != null) {
         cache.put(cacheKey, valueToCache);
      }

      return result;
   }
}
