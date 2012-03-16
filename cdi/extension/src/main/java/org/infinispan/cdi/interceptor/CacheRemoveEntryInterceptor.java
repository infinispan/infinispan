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
import org.infinispan.cdi.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.cache.annotation.CacheKey;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.cache.annotation.CacheRemoveEntry;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import java.io.Serializable;

/**
 * <p>{@link CacheRemoveEntry} interceptor implementation.This interceptor uses the following algorithm describes in
 * JSR-107.</p>
 *
 * <p>The interceptor that intercepts method annotated with {@code @CacheRemoveEntry} must do the following, generate a
 * key based on InvocationContext using the specified {@link CacheKeyGenerator}, use this key to remove the entry in the
 * cache. The remove occurs after the method body is executed. This can be overridden by specifying a afterInvocation
 * attribute value of false. If afterInvocation is true and the annotated method throws an exception the remove will not
 * happen.</p>
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Interceptor
public class CacheRemoveEntryInterceptor implements Serializable {

   private static final long serialVersionUID = -9079291622309963969L;
   private static final Log log = LogFactory.getLog(CacheRemoveEntryInterceptor.class, Log.class);


   private final CacheResolver cacheResolver;
   private final CacheKeyInvocationContextFactory contextFactory;

   @Inject
   public CacheRemoveEntryInterceptor(CacheResolver cacheResolver, CacheKeyInvocationContextFactory contextFactory) {
      this.cacheResolver = cacheResolver;
      this.contextFactory = contextFactory;
   }

   @AroundInvoke
   public Object cacheRemoveEntry(InvocationContext invocationContext) throws Exception {
      if (log.isTraceEnabled()) {
         log.tracef("Interception of method named '%s'", invocationContext.getMethod().getName());
      }

      final CacheKeyInvocationContext<CacheRemoveEntry> cacheKeyInvocationContext = contextFactory.getCacheKeyInvocationContext(invocationContext);
      final CacheKeyGenerator cacheKeyGenerator = cacheKeyInvocationContext.unwrap(CacheKeyInvocationContextImpl.class).getCacheKeyGenerator();
      final Cache<CacheKey, Object> cache = cacheResolver.resolveCache(cacheKeyInvocationContext);
      final CacheRemoveEntry cacheRemoveEntry = cacheKeyInvocationContext.getCacheAnnotation();
      final CacheKey cacheKey = cacheKeyGenerator.generateCacheKey(cacheKeyInvocationContext);

      if (!cacheRemoveEntry.afterInvocation()) {
         cache.remove(cacheKey);
         if (log.isTraceEnabled()) {
            log.tracef("Remove entry with key '%s' in cache '%s' before method invocation", cacheKey, cache.getName());
         }
      }

      final Object result = invocationContext.proceed();

      if (cacheRemoveEntry.afterInvocation()) {
         cache.remove(cacheKey);
         if (log.isTraceEnabled()) {
            log.tracef("Remove entry with key '%s' in cache '%s' after method invocation", cacheKey, cache.getName());
         }
      }

      return result;
   }
}
