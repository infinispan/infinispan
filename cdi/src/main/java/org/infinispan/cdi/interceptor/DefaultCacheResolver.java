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
import org.infinispan.manager.EmbeddedCacheManager;

import javax.cache.interceptor.CacheInvocationContext;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Instance;
import javax.enterprise.util.AnnotationLiteral;
import javax.inject.Inject;
import java.lang.annotation.Annotation;

import static org.infinispan.cdi.util.Contracts.assertNotNull;

/**
 * Default {@link CacheResolver} implementation.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class DefaultCacheResolver implements CacheResolver {

   private final EmbeddedCacheManager defaultCacheManager;
   private final Instance<EmbeddedCacheManager> cacheManagers;

   @Inject
   public DefaultCacheResolver(@Any Instance<EmbeddedCacheManager> cacheManagers) {
      this.cacheManagers = cacheManagers;
      this.defaultCacheManager = cacheManagers.select(new AnnotationLiteral<Default>() {}).get();
   }

   @Override
   public <K, V> Cache<K, V> resolveCache(CacheInvocationContext<? extends Annotation> cacheInvocationContext) {
      assertNotNull(cacheInvocationContext, "cacheInvocationContext parameter cannot be null");

      final String cacheName = cacheInvocationContext.getCacheName();

      // if the cache name is empty the default cache of the default cache manager is returned.
      if (cacheName.trim().isEmpty()) {
         return defaultCacheManager.getCache();
      }

      // here we need to iterate on all cache managers because the cache used by the interceptor could use a specific
      // cache manager.
      for (EmbeddedCacheManager oneCacheManager : cacheManagers) {
         if (oneCacheManager.getCacheNames().contains(cacheName)) {
            return oneCacheManager.getCache(cacheName);
         }
      }

      // if the cache has not been defined in the default cache manager or in a specific one a new cache is created in
      // the default cache manager with the default configuration.
      return defaultCacheManager.getCache(cacheName);
   }
}
